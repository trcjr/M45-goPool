package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type sv2SubmitBlockRPC struct {
	calls int32
}

func (r *sv2SubmitBlockRPC) callCtx(ctx context.Context, method string, params any, out any) error {
	if method != "submitblock" {
		return fmt.Errorf("unexpected method: %s", method)
	}
	atomic.AddInt32(&r.calls, 1)
	if p, ok := out.(*any); ok {
		*p = nil
	}
	return nil
}

func maxUint256ForTest(t *testing.T) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(strings.Repeat("f", 64), 16)
	if !ok {
		t.Fatalf("failed to build max uint256")
	}
	return n
}

func TestSV2BlockSubmitCountsAsAcceptedShare(t *testing.T) {
	rpc := &sv2SubmitBlockRPC{}
	metrics := NewPoolMetrics()

	worker := "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-block"
	c := &sv2Conn{
		id:         "sv2-test",
		conn:       nopConn{},
		rpc:        rpc,
		cfg:        Config{PoolFeePercent: 0},
		metrics:    metrics,
		workerName: worker,
		extranonce1: []byte{0x01, 0x02, 0x03, 0x04},
		difficulty: 1,
	}
	c.assignConnectionSeq()

	job := &Job{
		JobID:                   "sv2-block-job",
		Template:                GetBlockTemplateResult{Height: 101, Version: 0x20000000, CurTime: 1700000000, Bits: "1d00ffff", Previous: strings.Repeat("0", 64), CoinbaseValue: 50 * 1e8},
		Target:                  maxUint256ForTest(t),
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "sv2-block-test",
		ScriptTime:              0,
	}

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
		t.Fatalf("buildCoinbaseParts: %v", err)
	}
	coinb1Raw, err := hex.DecodeString(coinb1)
	if err != nil {
		t.Fatalf("decode coinb1: %v", err)
	}
	coinb2Raw, err := hex.DecodeString(coinb2)
	if err != nil {
		t.Fatalf("decode coinb2: %v", err)
	}
	standardExtranonce := make([]byte, 0, len(c.extranonce1)+job.Extranonce2Size)
	standardExtranonce = append(standardExtranonce, c.extranonce1...)
	if job.Extranonce2Size > 0 {
		standardExtranonce = append(standardExtranonce, make([]byte, job.Extranonce2Size)...)
	}
	standardCoinbase := make([]byte, 0, len(coinb1Raw)+len(standardExtranonce)+len(coinb2Raw))
	standardCoinbase = append(standardCoinbase, coinb1Raw...)
	standardCoinbase = append(standardCoinbase, standardExtranonce...)
	standardCoinbase = append(standardCoinbase, coinb2Raw...)

	const jobID uint32 = 7
	c.activeJobs.Store(jobID, &sv2JobInfo{job: job, coinb1: coinb1, coinb2: coinb2, standardExtranonce: standardExtranonce, standardCoinbaseTx: standardCoinbase})

	submit := &sv2SubmitSharesStandard{
		ChannelID:      1,
		SequenceNumber: 11,
		JobID:          jobID,
		Nonce:          0,
		NTime:          uint32(job.Template.CurTime),
		Version:        uint32(job.Template.Version),
	}
	c.handleSubmit(submit, nil, nil)

	if got := atomic.LoadInt32(&rpc.calls); got != 1 {
		t.Fatalf("submitblock calls = %d, want 1", got)
	}

	accepted, rejected, _ := metrics.Snapshot()
	if accepted != 1 || rejected != 0 {
		t.Fatalf("share metrics accepted/rejected = %d/%d, want 1/0", accepted, rejected)
	}
	_, _, blocksAccepted, blocksErrored, _, _, _, _, _, _, _, _ := metrics.SnapshotDiagnostics()
	if blocksAccepted != 1 || blocksErrored != 0 {
		t.Fatalf("block metrics accepted/errored = %d/%d, want 1/0", blocksAccepted, blocksErrored)
	}

	snap := c.snapshotShareInfo(time.Now())
	if snap.Stats.Accepted != 1 || snap.Stats.Rejected != 0 {
		t.Fatalf("conn stats accepted/rejected = %d/%d, want 1/0", snap.Stats.Accepted, snap.Stats.Rejected)
	}
	if !snap.LastShareAccepted {
		t.Fatalf("expected last share accepted on block-valid path")
	}
	if got := atomic.LoadUint32(&c.sequenceAck); got != submit.SequenceNumber {
		t.Fatalf("sequenceAck = %d, want %d", got, submit.SequenceNumber)
	}
}
