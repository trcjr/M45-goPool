package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type sv2Conn struct {
	id             string
	conn           net.Conn
	reader         *bufio.Reader
	jobMgr         *JobManager
	rpc            rpcCaller
	cfg            Config
	metrics        *PoolMetrics
	accounting     *AccountStore
	workerLists    *workerListStore
	workerRegistry *workerConnectionRegistry
	extranonce1    []byte
	extranonce1Hex string
	jobCh          chan *Job
	channelID      uint32
	workerName     string
	difficulty     float64
	activeJobs     sync.Map  // uint32 jobID → *sv2JobInfo
	nextJobID      uint32
	sequenceAck    uint32
	cleanupOnce    sync.Once
	connectedAt    time.Time
	isTLS          bool
	writeMu        sync.Mutex
}

func NewSV2Conn(conn net.Conn, jobMgr *JobManager, rpc rpcCaller, cfg Config, metrics *PoolMetrics, accounting *AccountStore, workerRegistry *workerConnectionRegistry, workerLists *workerListStore, isTLS bool) *sv2Conn {
	en1 := jobMgr.NextExtranonce1()
	diff := cfg.MinDifficulty
	if diff <= 0 {
		diff = defaultMinDifficulty
	}
	return &sv2Conn{
		id:             conn.RemoteAddr().String(),
		conn:           conn,
		reader:         bufio.NewReaderSize(conn, maxStratumMessageSize),
		jobMgr:         jobMgr,
		rpc:            rpc,
		cfg:            cfg,
		metrics:        metrics,
		accounting:     accounting,
		workerLists:    workerLists,
		workerRegistry: workerRegistry,
		extranonce1:    en1,
		extranonce1Hex: hex.EncodeToString(en1),
		channelID:      1,
		difficulty:     diff,
		connectedAt:    time.Now(),
		isTLS:          isTLS,
	}
}

func (c *sv2Conn) cleanup() {
	c.cleanupOnce.Do(func() {
		if c.jobMgr != nil && c.jobCh != nil {
			c.jobMgr.Unsubscribe(c.jobCh)
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *sv2Conn) writeFrame(msgType byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return sv2WriteFrame(c.conn, msgType, payload)
}

func (c *sv2Conn) readFrame() (byte, []byte, error) {
	return sv2ReadFrame(c.reader)
}

func (c *sv2Conn) nextSV2JobID() uint32 {
	return atomic.AddUint32(&c.nextJobID, 1)
}

func (c *sv2Conn) handle() {
	defer c.cleanup()

	logger.Info("sv2 connection", "component", "stratum", "kind", "sv2", "remote", c.id)

	// 1. Read SetupConnection
	msgType, payload, err := c.readFrame()
	if err != nil {
		logger.Warn("sv2 read setup", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	if msgType != sv2MsgSetupConnection {
		logger.Warn("sv2 expected SetupConnection", "component", "stratum", "kind", "sv2", "remote", c.id, "got", msgType)
		return
	}
	var setup sv2SetupConnection
	if err := setup.decode(payload); err != nil {
		logger.Warn("sv2 decode SetupConnection", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	// Protocol 0x00 = Mining Protocol
	if setup.Protocol != 0x00 {
		errMsg := sv2SetupConnectionError{Flags: 0, ErrorCode: "unsupported-feature-flags"}
		_ = c.writeFrame(sv2MsgSetupConnectionError, errMsg.encode())
		return
	}

	// 2. Send SetupConnectionSuccess
	success := sv2SetupConnectionSuccess{UsedVersion: 2, Flags: 0}
	if err := c.writeFrame(sv2MsgSetupConnectionSuccess, success.encode()); err != nil {
		return
	}

	// 3. Read OpenStandardMiningChannel
	msgType, payload, err = c.readFrame()
	if err != nil {
		logger.Warn("sv2 read open channel", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	if msgType != sv2MsgOpenStandardMiningChannel {
		logger.Warn("sv2 expected OpenStandardMiningChannel", "component", "stratum", "kind", "sv2", "remote", c.id, "got", msgType)
		return
	}
	var openCh sv2OpenStandardMiningChannel
	if err := openCh.decode(payload); err != nil {
		logger.Warn("sv2 decode OpenChannel", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	c.workerName = openCh.UserIdentity

	target := sv2TargetFromDifficulty(c.difficulty)

	// 4. Send OpenStdMiningChannelSuccess
	chanSuccess := sv2OpenStdMiningChannelSuccess{
		RequestID:        openCh.RequestID,
		ChannelID:        c.channelID,
		Target:           target,
		ExtraNoncePrefix: c.extranonce1,
		GroupChannelID:   0,
	}
	if err := c.writeFrame(sv2MsgOpenStdMiningChannelSuccess, chanSuccess.encode()); err != nil {
		return
	}

	logger.Info("sv2 channel opened", "component", "stratum", "kind", "sv2",
		"remote", c.id, "worker", c.workerName, "channel_id", c.channelID)

	// 5. Subscribe to job feed and send current job
	c.jobCh = c.jobMgr.Subscribe()
	curJob := c.jobMgr.CurrentJob()
	if curJob != nil {
		if err := c.sendJob(curJob); err != nil {
			logger.Warn("sv2 send initial job", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			return
		}
	}

	// 6. Goroutine: read frames from miner
	submitCh := make(chan *sv2SubmitSharesStandard, 16)
	errCh := make(chan error, 2)

	go func() {
		for {
			mt, pay, err := c.readFrame()
			if err != nil {
				errCh <- err
				return
			}
			if mt == sv2MsgSubmitSharesStandard {
				var submit sv2SubmitSharesStandard
				if decErr := submit.decode(pay); decErr != nil {
					logger.Warn("sv2 decode submit", "component", "stratum", "kind", "sv2", "remote", c.id, "error", decErr)
					continue
				}
				submitCh <- &submit
			}
		}
	}()

	// 7. Main loop: handle job updates and submits
	for {
		select {
		case err := <-errCh:
			if err != nil {
				logger.Debug("sv2 conn closed", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			}
			return

		case job, ok := <-c.jobCh:
			if !ok {
				return
			}
			if job == nil {
				continue
			}
			if err := c.sendJob(job); err != nil {
				logger.Warn("sv2 send job", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
				return
			}

		case submit, ok := <-submitCh:
			if !ok {
				return
			}
			c.handleSubmit(submit)
		}
	}
}

func (c *sv2Conn) sendJob(job *Job) error {
	if job == nil {
		return nil
	}
	// Build coinbase parts; extranonce2 is all zeros for SV2 standard channels
	coinb1, coinb2, err := buildCoinbaseParts(
		job.Template.Height,
		c.extranonce1,
		job.Extranonce2Size,
		job.TemplateExtraNonce2Size,
		job.PayoutScript,
		job.CoinbaseValue,
		job.WitnessCommitment,
		job.Template.CoinbaseAux.Flags,
		job.CoinbaseMsg,
		job.ScriptTime,
	)
	if err != nil {
		return fmt.Errorf("sv2 build coinbase: %w", err)
	}

	jobID := c.nextSV2JobID()
	info := &sv2JobInfo{job: job, coinb1: coinb1, coinb2: coinb2}
	c.activeJobs.Store(jobID, info)

	prevHashMsg := sv2BuildSetNewPrevHash(job, c.channelID, jobID)
	if err := c.writeFrame(sv2MsgSetNewPrevHash, prevHashMsg.encode()); err != nil {
		return err
	}

	jobMsg := sv2BuildNewMiningJob(info, c.channelID, jobID, false)
	return c.writeFrame(sv2MsgNewMiningJob, jobMsg.encode())
}

func (c *sv2Conn) handleSubmit(submit *sv2SubmitSharesStandard) {
	jobIDAny, ok := c.activeJobs.Load(submit.JobID)
	if !ok {
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "job-not-found")
		return
	}
	info := jobIDAny.(*sv2JobInfo)
	job := info.job

	// In SV2 standard channels extranonce2 is always zeros (extranonce1 differentiates miners)
	en2 := make([]byte, job.Extranonce2Size)
	ntimeHex := fmt.Sprintf("%08x", submit.NTime)
	nonceHex := fmt.Sprintf("%08x", submit.Nonce)

	blockHex, headerHashLE, _, _, err := buildBlockWithScriptTime(
		job, c.extranonce1, en2, ntimeHex, nonceHex, int32(submit.Version),
		job.PayoutScript, job.ScriptTime,
	)
	if err != nil {
		logger.Warn("sv2 build block", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "internal-error")
		return
	}

	// headerHashLE is little-endian; reverse for big-endian display/comparison
	hashBE := reverseBytes(headerHashLE)
	hashHex := hex.EncodeToString(hashBE)
	hashInt := new(big.Int).SetBytes(hashBE)

	shareTarget := targetFromDifficulty(c.difficulty)
	if hashInt.Cmp(shareTarget) > 0 {
		logger.Debug("sv2 low-diff share", "component", "stratum", "kind", "sv2",
			"remote", c.id, "worker", c.workerName, "hash", hashHex)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "low-difficulty")
		return
	}

	isBlock := job.Target != nil && hashInt.Cmp(job.Target) <= 0
	logger.Info("sv2 share accepted", "component", "stratum", "kind", "sv2",
		"remote", c.id, "worker", c.workerName,
		"job_id", submit.JobID, "hash", hashHex, "is_block", isBlock)

	if isBlock {
		var submitRes interface{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.rpc.callCtx(ctx, "submitblock", []any{blockHex}, &submitRes); err != nil {
			logger.Error("sv2 block submit failed", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName, "hash", hashHex, "error", err)
		} else {
			logger.Info("sv2 BLOCK FOUND", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName, "hash", hashHex, "height", job.Template.Height)
		}
	}

	atomic.StoreUint32(&c.sequenceAck, submit.SequenceNumber)
	c.sendShareSuccess(submit.ChannelID, submit.SequenceNumber)
}

func (c *sv2Conn) sendShareSuccess(channelID, seqNum uint32) {
	msg := sv2SubmitSharesSuccess{
		ChannelID:               channelID,
		LastSequenceNumber:      seqNum,
		NewSubmitsAcceptedCount: 1,
		NewSharesSum:            uint64(c.difficulty),
	}
	_ = c.writeFrame(sv2MsgSubmitSharesSuccess, msg.encode())
}

func (c *sv2Conn) sendShareError(channelID, seqNum uint32, errCode string) {
	msg := sv2SubmitSharesError{
		ChannelID:      channelID,
		SequenceNumber: seqNum,
		ErrorCode:      errCode,
	}
	_ = c.writeFrame(sv2MsgSubmitSharesError, msg.encode())
}

