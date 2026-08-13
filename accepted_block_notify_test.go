package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type trackedConn struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (c *trackedConn) Read([]byte) (int, error)         { return 0, nil }
func (c *trackedConn) LocalAddr() net.Addr              { return &net.IPAddr{} }
func (c *trackedConn) RemoteAddr() net.Addr             { return &net.IPAddr{} }
func (c *trackedConn) SetDeadline(time.Time) error      { return nil }
func (c *trackedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *trackedConn) SetWriteDeadline(time.Time) error { return nil }

func (c *trackedConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(b)
}

func (c *trackedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *trackedConn) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *trackedConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func newListenerMinerForAcceptedBlockTest(t *testing.T, jm *JobManager, workerName string) (*MinerConn, *trackedConn) {
	t.Helper()
	_, workerWallet, workerScript := generateTestWorker(t)
	conn := &trackedConn{}
	jobCh := jm.Subscribe()
	mc := &MinerConn{
		id:             workerName + "-remote",
		conn:           conn,
		cfg:            Config{PoolFeePercent: 0},
		jobMgr:         jm,
		jobCh:          jobCh,
		extranonce1:    []byte{0x01, 0x02, 0x03, 0x04},
		authorized:     true,
		subscribed:     true,
		listenerOn:     true,
		lockDifficulty: true,
		stats: MinerStats{
			Worker:       workerName,
			WorkerSHA256: workerNameHash(workerName),
		},
		activeJobs:        make(map[string]*Job, 16),
		jobOrder:          make([]string, 0, 16),
		jobDifficulty:     make(map[string]float64, 16),
		jobScriptTime:     make(map[string]int64, 16),
		jobNotifyCoinbase: make(map[string]notifiedCoinbaseParts, 16),
		maxRecentJobs:     16,
	}
	atomicStoreFloat64(&mc.difficulty, 1)
	mc.shareTarget.Store(targetFromDifficulty(1))
	mc.setWorkerWallet(workerName, workerWallet, workerScript)
	go mc.listenJobs()
	t.Cleanup(func() {
		jm.Unsubscribe(jobCh)
	})
	return mc, conn
}

func waitForNotifyCount(t *testing.T, conn *trackedConn, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(notifyMessagesFromOutput(t, conn.String())); got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d notify messages; got output: %q", want, conn.String())
}

func latestNotifyCleanFlag(t *testing.T, conn *trackedConn) bool {
	t.Helper()
	msgs := notifyMessagesFromOutput(t, conn.String())
	if len(msgs) == 0 {
		t.Fatalf("expected at least one notify message")
	}
	params := msgs[len(msgs)-1].Params
	if len(params) < 9 {
		t.Fatalf("notify params too short: %#v", params)
	}
	clean, ok := params[8].(bool)
	if !ok {
		t.Fatalf("notify clean_jobs is not bool: %#v", params[8])
	}
	return clean
}

func TestAcceptedBlockRefreshBroadcastsCleanNotifyToAuthorizedMiners(t *testing.T) {
	current := benchmarkSubmitJobForTest(t)
	current.JobID = "accepted-old-job"
	current.Template.Height = 700
	current.Template.Previous = strings.Repeat("1", 64)

	nextTpl := GetBlockTemplateResult{
		Height:                   701,
		CurTime:                  current.Template.CurTime + 1,
		Mintime:                  current.Template.CurTime + 1,
		Bits:                     current.Template.Bits,
		Previous:                 strings.Repeat("2", 64),
		Version:                  current.Template.Version,
		CoinbaseValue:            current.Template.CoinbaseValue,
		DefaultWitnessCommitment: "00",
		Transactions:             current.Template.Transactions,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		resp := rpcResponse{ID: req.ID}
		switch req.Method {
		case "getblocktemplate":
			data, _ := json.Marshal(nextTpl)
			resp.Result = data
		case "getbestblockhash":
			data, _ := json.Marshal(strings.Repeat("2", 64))
			resp.Result = data
		case "getblockheader":
			header := BlockHeader{Hash: strings.Repeat("2", 64), Height: 701, Time: nextTpl.CurTime, PreviousBlockHash: strings.Repeat("1", 64), Bits: nextTpl.Bits, Difficulty: 1}
			data, _ := json.Marshal(header)
			resp.Result = data
		default:
			resp.Error = &rpcError{Code: -32601, Message: "method not found"}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	rpc := &RPCClient{url: srv.URL, client: srv.Client(), lp: srv.Client()}
	jm := NewJobManager(rpc, Config{Extranonce2Size: 4, TemplateExtraNonce2Size: 8}, nil, []byte{0x51}, nil)
	jm.curJob = current

	_, conn1 := newListenerMinerForAcceptedBlockTest(t, jm, "worker-a")
	_, conn2 := newListenerMinerForAcceptedBlockTest(t, jm, "worker-b")

	unauthConn := &trackedConn{}
	unauthCh := jm.Subscribe()
	unauth := &MinerConn{
		id:         "worker-unauth",
		conn:       unauthConn,
		jobMgr:     jm,
		jobCh:      unauthCh,
		subscribed: true,
		authorized: false,
		listenerOn: false,
	}
	_ = unauth
	t.Cleanup(func() { jm.Unsubscribe(unauthCh) })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := jm.refreshAfterAcceptedBlock(ctx, current.Template.Height); err != nil {
		t.Fatalf("refreshAfterAcceptedBlock error: %v", err)
	}

	job := jm.CurrentJob()
	if job == nil {
		t.Fatalf("expected current job after accepted block refresh")
	}
	if job.Template.Height != 701 {
		t.Fatalf("expected new height 701, got %d", job.Template.Height)
	}
	if job.NotifyReason != "accepted_block" {
		t.Fatalf("expected notify reason accepted_block, got %q", job.NotifyReason)
	}

	waitForNotifyCount(t, conn1, 1)
	waitForNotifyCount(t, conn2, 1)
	if !latestNotifyCleanFlag(t, conn1) {
		t.Fatalf("expected clean_jobs=true for miner 1 on accepted block notify")
	}
	if !latestNotifyCleanFlag(t, conn2) {
		t.Fatalf("expected clean_jobs=true for miner 2 on accepted block notify")
	}

	if len(notifyMessagesFromOutput(t, unauthConn.String())) != 0 {
		t.Fatalf("unauthorized subscribed miner should not receive notify before authorize: %q", unauthConn.String())
	}

	before1 := len(notifyMessagesFromOutput(t, conn1.String()))
	before2 := len(notifyMessagesFromOutput(t, conn2.String()))

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := jm.refreshAfterAcceptedBlock(ctx2, current.Template.Height); err != nil {
		t.Fatalf("second refreshAfterAcceptedBlock error: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	after1 := len(notifyMessagesFromOutput(t, conn1.String()))
	after2 := len(notifyMessagesFromOutput(t, conn2.String()))
	if after1 != before1 || after2 != before2 {
		t.Fatalf("expected no duplicate accepted-block notify storm; before=(%d,%d) after=(%d,%d)", before1, before2, after1, after2)
	}
}

func TestHandleBlockShareRespondsBeforeAcceptedBlockNotify(t *testing.T) {
	workerName, workerWallet, workerScript := generateTestWorker(t)
	job := benchmarkSubmitJobForTest(t)
	job.JobID = "handle-block-order-job"
	job.Template.Height = 900
	job.Template.Previous = strings.Repeat("a", 64)

	nextTpl := GetBlockTemplateResult{
		Height:                   901,
		CurTime:                  job.Template.CurTime + 1,
		Mintime:                  job.Template.CurTime + 1,
		Bits:                     job.Template.Bits,
		Previous:                 strings.Repeat("b", 64),
		Version:                  job.Template.Version,
		CoinbaseValue:            job.Template.CoinbaseValue,
		DefaultWitnessCommitment: "00",
		Transactions:             job.Template.Transactions,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode rpc request: %v", err)
		}
		resp := rpcResponse{ID: req.ID}
		switch req.Method {
		case "submitblock":
			resp.Result = nil
		case "getblocktemplate":
			data, _ := json.Marshal(nextTpl)
			resp.Result = data
		case "getbestblockhash":
			data, _ := json.Marshal(strings.Repeat("b", 64))
			resp.Result = data
		case "getblockheader":
			header := BlockHeader{Hash: strings.Repeat("b", 64), Height: 901, Time: nextTpl.CurTime, PreviousBlockHash: strings.Repeat("a", 64), Bits: nextTpl.Bits, Difficulty: 1}
			data, _ := json.Marshal(header)
			resp.Result = data
		default:
			resp.Error = &rpcError{Code: -32601, Message: "method not found"}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	rpc := &RPCClient{url: srv.URL, client: srv.Client(), lp: srv.Client()}
	jm := NewJobManager(rpc, Config{Extranonce2Size: 4, TemplateExtraNonce2Size: 8}, nil, []byte{0x51}, nil)
	jm.curJob = job

	conn := &trackedConn{}
	jobCh := jm.Subscribe()
	mc := &MinerConn{
		id:             "submitter",
		conn:           conn,
		rpc:            rpc,
		jobMgr:         jm,
		jobCh:          jobCh,
		cfg:            Config{PoolFeePercent: 0},
		extranonce1:    []byte{0x01, 0x02, 0x03, 0x04},
		authorized:     true,
		subscribed:     true,
		listenerOn:     true,
		lockDifficulty: true,
		stats: MinerStats{
			Worker:       workerName,
			WorkerSHA256: workerNameHash(workerName),
		},
		activeJobs:        make(map[string]*Job, 16),
		jobOrder:          make([]string, 0, 16),
		jobDifficulty:     make(map[string]float64, 16),
		jobScriptTime:     make(map[string]int64, 16),
		jobNotifyCoinbase: make(map[string]notifiedCoinbaseParts, 16),
		maxRecentJobs:     16,
	}
	atomicStoreFloat64(&mc.difficulty, 1)
	mc.shareTarget.Store(targetFromDifficulty(1))
	mc.setWorkerWallet(workerName, workerWallet, workerScript)
	go mc.listenJobs()
	t.Cleanup(func() { jm.Unsubscribe(jobCh) })

	mc.handleBlockShare(1, job, job.JobID, workerName, []byte{0x00, 0x00, 0x00, 0x01}, uint32ToHex8Lower(uint32(job.Template.CurTime)), "00000001", uint32(job.Template.Version), job.ScriptTime, nil, nil, strings.Repeat("0", 64), 1.0, time.Now())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(conn.String(), "\"method\":\"mining.notify\"") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	lines := nonEmptyLines(conn.String())
	if len(lines) < 2 {
		t.Fatalf("expected submit response and notify, got lines: %#v", lines)
	}
	if strings.Contains(lines[0], "\"method\":\"mining.notify\"") {
		t.Fatalf("expected submit response before notify, got: %q", lines[0])
	}
	if !strings.Contains(lines[0], "\"result\":true") {
		t.Fatalf("expected successful submit response first, got: %q", lines[0])
	}
	if !strings.Contains(conn.String(), "\"method\":\"mining.notify\"") {
		t.Fatalf("expected follow-up notify after accepted block, got: %q", conn.String())
	}
	if !latestNotifyCleanFlag(t, conn) {
		t.Fatalf("expected accepted-block follow-up notify with clean_jobs=true")
	}
	if conn.IsClosed() {
		t.Fatalf("expected miner session to remain open")
	}
}
