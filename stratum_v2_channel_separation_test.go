package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type capturedSV2Frame struct {
	msgType byte
	payload []byte
}

type sv2FrameCaptureConn struct {
	frames []capturedSV2Frame
}

func (c *sv2FrameCaptureConn) WriteSV2Frame(msgType byte, payload []byte) error {
	cp := append([]byte(nil), payload...)
	c.frames = append(c.frames, capturedSV2Frame{msgType: msgType, payload: cp})
	return nil
}

func (c *sv2FrameCaptureConn) Read([]byte) (int, error)         { return 0, nil }
func (c *sv2FrameCaptureConn) Write([]byte) (int, error)        { return 0, fmt.Errorf("unsupported") }
func (c *sv2FrameCaptureConn) Close() error                     { return nil }
func (c *sv2FrameCaptureConn) LocalAddr() net.Addr              { return &net.IPAddr{} }
func (c *sv2FrameCaptureConn) RemoteAddr() net.Addr             { return &net.IPAddr{} }
func (c *sv2FrameCaptureConn) SetDeadline(time.Time) error      { return nil }
func (c *sv2FrameCaptureConn) SetReadDeadline(time.Time) error  { return nil }
func (c *sv2FrameCaptureConn) SetWriteDeadline(time.Time) error { return nil }

type scriptedSV2Conn struct {
	mu      sync.Mutex
	reads   []capturedSV2Frame
	readIdx int
	writes  []capturedSV2Frame
}

func (c *scriptedSV2Conn) ReadSV2Frame() (byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readIdx >= len(c.reads) {
		return 0, nil, io.EOF
	}
	f := c.reads[c.readIdx]
	c.readIdx++
	return f.msgType, append([]byte(nil), f.payload...), nil
}

func (c *scriptedSV2Conn) WriteSV2Frame(msgType byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]byte(nil), payload...)
	c.writes = append(c.writes, capturedSV2Frame{msgType: msgType, payload: cp})
	return nil
}

func (c *scriptedSV2Conn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *scriptedSV2Conn) Write([]byte) (int, error)        { return 0, fmt.Errorf("unsupported") }
func (c *scriptedSV2Conn) Close() error                     { return nil }
func (c *scriptedSV2Conn) LocalAddr() net.Addr              { return &net.IPAddr{} }
func (c *scriptedSV2Conn) RemoteAddr() net.Addr             { return &net.IPAddr{} }
func (c *scriptedSV2Conn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedSV2Conn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedSV2Conn) SetWriteDeadline(time.Time) error { return nil }

func testSV2SendJobFixture() *Job {
	return &Job{
		JobID:                   "sv2-separation-job",
		Template:                GetBlockTemplateResult{Height: 101, Version: 0x20000000, CurTime: 1700000000, Mintime: 1700000000, Bits: "1d00ffff", Previous: strings.Repeat("0", 64), CoinbaseValue: 50 * 1e8},
		Target:                  nil,
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "sv2-separation-test",
		ScriptTime:              0,
	}
}

func TestSV2SendJobUsesNewMiningJobOnStandardChannels(t *testing.T) {
	capture := &sv2FrameCaptureConn{}
	c := &sv2Conn{
		id:          "sv2-standard-test",
		conn:        capture,
		cfg:         Config{PoolFeePercent: 0},
		workerName:  "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-std",
		extranonce1: []byte{0x01, 0x02, 0x03, 0x04},
		channelID:   1,
		channelType: sv2ChannelTypeStandard,
	}
	job := testSV2SendJobFixture()

	if err := c.sendJob(job); err != nil {
		t.Fatalf("sendJob: %v", err)
	}
	if len(capture.frames) < 2 {
		t.Fatalf("expected at least 2 frames, got %d", len(capture.frames))
	}
	if capture.frames[0].msgType != sv2MsgNewMiningJob {
		t.Fatalf("first message type = 0x%02x, want 0x%02x (NewMiningJob)", capture.frames[0].msgType, sv2MsgNewMiningJob)
	}
	var msg sv2NewMiningJob
	if err := msg.decode(capture.frames[0].payload); err != nil {
		t.Fatalf("decode NewMiningJob: %v", err)
	}
	if msg.ChannelID != c.channelID {
		t.Fatalf("channel_id = %d, want %d", msg.ChannelID, c.channelID)
	}
}

func TestSV2SendJobUsesNewExtendedMiningJobOnExtendedChannels(t *testing.T) {
	capture := &sv2FrameCaptureConn{}
	c := &sv2Conn{
		id:            "sv2-extended-test",
		conn:          capture,
		cfg:           Config{PoolFeePercent: 0},
		workerName:    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-ext",
		extranonce1:   []byte{0x01, 0x02, 0x03, 0x04},
		extranonceSize: 4,
		isExtended:    true,
		channelID:     1,
		channelType:   sv2ChannelTypeExtended,
	}
	job := testSV2SendJobFixture()

	if err := c.sendJob(job); err != nil {
		t.Fatalf("sendJob: %v", err)
	}
	if len(capture.frames) < 2 {
		t.Fatalf("expected at least 2 frames, got %d", len(capture.frames))
	}
	if capture.frames[0].msgType != sv2MsgNewExtendedMiningJob {
		t.Fatalf("first message type = 0x%02x, want 0x%02x (NewExtendedMiningJob)", capture.frames[0].msgType, sv2MsgNewExtendedMiningJob)
	}
	var msg sv2NewExtendedMiningJob
	if err := msg.decode(capture.frames[0].payload); err != nil {
		t.Fatalf("decode NewExtendedMiningJob: %v", err)
	}
	if msg.ChannelID != c.channelID {
		t.Fatalf("channel_id = %d, want %d", msg.ChannelID, c.channelID)
	}
}

func TestSV2HandleRejectsExtendedSubmitOnStandardChannel(t *testing.T) {
	setup := (&sv2SetupConnection{
		Protocol:     0x00,
		MinVersion:   2,
		MaxVersion:   2,
		EndpointHost: "127.0.0.1",
		EndpointPort: 3333,
		Vendor:       "test",
		Firmware:     "1.0",
	}).encode()
	openStd := (&sv2OpenStandardMiningChannel{
		RequestID:       1,
		UserIdentity:    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-loop",
		NominalHashRate: 1,
	}).encode()
	badSubmit := (&sv2SubmitSharesExtended{
		ChannelID:      1,
		SequenceNumber: 7,
		JobID:          42,
		Nonce:          1,
		NTime:          2,
		Version:        3,
		Extranonce:     []byte{0xaa, 0xbb},
	}).encode()

	conn := &scriptedSV2Conn{reads: []capturedSV2Frame{
		{msgType: sv2MsgSetupConnection, payload: setup},
		{msgType: sv2MsgOpenStandardMiningChannel, payload: openStd},
		{msgType: sv2MsgSubmitSharesExtended, payload: badSubmit},
	}}

	jm := NewJobManager(nil, Config{}, nil, nil, nil)
	c := &sv2Conn{
		id:        "sv2-handle-loop-test",
		conn:      conn,
		jobMgr:    jm,
		cfg:       Config{},
		channelID: 1,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.handle()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handle did not exit")
	}

	if len(conn.writes) < 2 {
		t.Fatalf("expected at least setup/open responses, got %d writes", len(conn.writes))
	}
	if conn.writes[0].msgType != sv2MsgSetupConnectionSuccess {
		t.Fatalf("first response type = 0x%02x, want 0x%02x", conn.writes[0].msgType, sv2MsgSetupConnectionSuccess)
	}
	if conn.writes[1].msgType != sv2MsgOpenStdMiningChannelSuccess {
		t.Fatalf("second response type = 0x%02x, want 0x%02x", conn.writes[1].msgType, sv2MsgOpenStdMiningChannelSuccess)
	}
	for i := 2; i < len(conn.writes); i++ {
		if conn.writes[i].msgType == sv2MsgSubmitSharesError || conn.writes[i].msgType == sv2MsgSubmitSharesSuccess {
			t.Fatalf("unexpected submit response on mismatch path: msgType=0x%02x", conn.writes[i].msgType)
		}
	}
}

type sv2SubmitCaptureRPC struct {
	calls   int32
	blocks  []string
}

func (r *sv2SubmitCaptureRPC) callCtx(_ context.Context, method string, params any, out any) error {
	if method != "submitblock" {
		return fmt.Errorf("unexpected method: %s", method)
	}
	atomic.AddInt32(&r.calls, 1)
	if pp, ok := params.([]any); ok && len(pp) == 1 {
		if blockHex, ok := pp[0].(string); ok {
			r.blocks = append(r.blocks, blockHex)
		}
	}
	if p, ok := out.(*any); ok {
		*p = nil
	}
	return nil
}

func maxUint256ForStandardTest(t *testing.T) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(strings.Repeat("f", 64), 16)
	if !ok {
		t.Fatal("failed to build max uint256")
	}
	return n
}

func decodeHeader80FromBlockHex(t *testing.T, blockHex string) []byte {
	t.Helper()
	b, err := hex.DecodeString(blockHex)
	if err != nil {
		t.Fatalf("decode block hex: %v", err)
	}
	if len(b) < 80 {
		t.Fatalf("block bytes too short: %d", len(b))
	}
	out := make([]byte, 80)
	copy(out, b[:80])
	return out
}

func txidWireLEFromHex(t *testing.T, txHex string) [32]byte {
	t.Helper()
	raw, err := hex.DecodeString(txHex)
	if err != nil {
		t.Fatalf("decode tx hex: %v", err)
	}
	return doubleSHA256Array(raw)
}

func TestSV2StandardOldJobSubmitUsesImmutablePerJobData(t *testing.T) {
	rpc := &sv2SubmitCaptureRPC{}
	conn := &sv2FrameCaptureConn{}
	c := &sv2Conn{
		id:          "sv2-standard-immutable-job-test",
		conn:        conn,
		rpc:         rpc,
		cfg:         Config{PoolFeePercent: 0},
		workerName:  "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-imm",
		extranonce1: []byte{0x00, 0x00, 0x00, 0x01},
		difficulty:  1,
		channelID:   1,
		channelType: sv2ChannelTypeStandard,
	}

	jobOld := &Job{
		JobID:                   "sv2-std-old",
		Template:                GetBlockTemplateResult{Height: 101, Version: 0x20000000, CurTime: 1700000001, Mintime: 1700000001, Bits: "1d00ffff", Previous: strings.Repeat("0", 64), CoinbaseValue: 50 * 1e8},
		Target:                  maxUint256ForStandardTest(t),
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "std-old",
		ScriptTime:              0,
	}
	jobNew := &Job{
		JobID:                   "sv2-std-new",
		Template:                GetBlockTemplateResult{Height: 102, Version: 0x20000000, CurTime: 1700000002, Mintime: 1700000002, Bits: "1d00ffff", Previous: strings.Repeat("1", 64), CoinbaseValue: 50 * 1e8},
		Target:                  maxUint256ForStandardTest(t),
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "std-new",
		ScriptTime:              0,
	}

	if err := c.sendJob(jobOld); err != nil {
		t.Fatalf("sendJob old: %v", err)
	}
	oldAny, ok := c.activeJobs.Load(uint32(1))
	if !ok {
		t.Fatal("old job not found")
	}
	oldInfo := oldAny.(*sv2JobInfo)
	if len(oldInfo.standardCoinbaseTx) == 0 {
		t.Fatal("old job missing immutable standard coinbase")
	}

	// Mutate connection-level prefix to ensure reconstruction uses immutable per-job data.
	c.extranonce1 = []byte{0xaa, 0xbb, 0xcc, 0xdd}
	c.extranonce1Hex = hex.EncodeToString(c.extranonce1)
	if err := c.sendJob(jobNew); err != nil {
		t.Fatalf("sendJob new: %v", err)
	}

	header80, err := buildBlockHeaderFromHex(
		int32(jobOld.Template.Version),
		jobOld.Template.Previous,
		oldInfo.standardMerkleRoot[:],
		fmt.Sprintf("%08x", uint32(jobOld.Template.CurTime)),
		jobOld.Template.Bits,
		fmt.Sprintf("%08x", uint32(1)),
	)
	if err != nil {
		t.Fatalf("build expected header: %v", err)
	}

	submit := &sv2SubmitSharesStandard{
		ChannelID:      1,
		SequenceNumber: 9,
		JobID:          1,
		Nonce:          1,
		NTime:          uint32(jobOld.Template.CurTime),
		Version:        uint32(jobOld.Template.Version),
	}
	c.handleSubmit(submit, nil, header80)

	if got := atomic.LoadInt32(&rpc.calls); got != 1 {
		t.Fatalf("submitblock calls = %d, want 1", got)
	}
	if len(rpc.blocks) != 1 {
		t.Fatalf("submitblock blocks = %d, want 1", len(rpc.blocks))
	}
	submittedHeader := decodeHeader80FromBlockHex(t, rpc.blocks[0])
	if hex.EncodeToString(submittedHeader) != hex.EncodeToString(header80) {
		t.Fatalf("submitted header mismatch: got %x want %x", submittedHeader, header80)
	}
}

func TestSV2StandardHeaderHintMismatchReturnsExplicitReason(t *testing.T) {
	rpc := &sv2SubmitCaptureRPC{}
	conn := &sv2FrameCaptureConn{}
	c := &sv2Conn{
		id:          "sv2-standard-hint-mismatch-test",
		conn:        conn,
		rpc:         rpc,
		cfg:         Config{PoolFeePercent: 0},
		workerName:  "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-hint",
		extranonce1: []byte{0x00, 0x00, 0x00, 0x01},
		difficulty:  1,
		channelID:   1,
		channelType: sv2ChannelTypeStandard,
	}
	job := &Job{
		JobID:                   "sv2-std-hint",
		Template:                GetBlockTemplateResult{Height: 103, Version: 0x20000000, CurTime: 1700000003, Mintime: 1700000003, Bits: "1d00ffff", Previous: strings.Repeat("0", 64), CoinbaseValue: 50 * 1e8},
		Target:                  maxUint256ForStandardTest(t),
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "std-hint",
		ScriptTime:              0,
	}
	if err := c.sendJob(job); err != nil {
		t.Fatalf("sendJob: %v", err)
	}

	wrongHeader := make([]byte, 80)
	copy(wrongHeader[:4], []byte{0x01, 0x02, 0x03, 0x04})
	submit := &sv2SubmitSharesStandard{
		ChannelID:      1,
		SequenceNumber: 10,
		JobID:          1,
		Nonce:          1,
		NTime:          uint32(job.Template.CurTime),
		Version:        uint32(job.Template.Version),
	}
	c.handleSubmit(submit, nil, wrongHeader)

	if got := atomic.LoadInt32(&rpc.calls); got != 0 {
		t.Fatalf("submitblock calls = %d, want 0", got)
	}
	if len(conn.frames) == 0 {
		t.Fatal("expected submit error frame")
	}
	last := conn.frames[len(conn.frames)-1]
	if last.msgType != sv2MsgSubmitSharesError {
		t.Fatalf("last msgType = 0x%02x, want 0x%02x", last.msgType, sv2MsgSubmitSharesError)
	}
	var errMsg sv2SubmitSharesError
	if err := errMsg.decode(last.payload); err != nil {
		t.Fatalf("decode submit error: %v", err)
	}
	if errMsg.ErrorCode != "job-data-mismatch" && errMsg.ErrorCode != "merkle-only-mismatch" && errMsg.ErrorCode != "header-mismatch" {
		t.Fatalf("unexpected error code: %q", errMsg.ErrorCode)
	}
}

func TestSV2StandardSubmitblockAssemblyMerkleMatchesHeader(t *testing.T) {
	rpc := &sv2SubmitCaptureRPC{}
	conn := &sv2FrameCaptureConn{}
	txHex := "010000000100000000000000000000000000000000000000000000000000000000000000000000000000ffffffff010100000000000000015100000000"
	txID := txidWireLEFromHex(t, txHex)
	job := &Job{
		JobID:                   "sv2-std-assembly-pass",
		Template:                GetBlockTemplateResult{Height: 104, Version: 0x20000000, CurTime: 1700000004, Mintime: 1700000004, Bits: "1d00ffff", Previous: strings.Repeat("0", 64), CoinbaseValue: 50 * 1e8},
		Target:                  maxUint256ForStandardTest(t),
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "std-assembly-pass",
		ScriptTime:              0,
		Transactions:            []GBTTransaction{{Data: txHex}},
		MerkleBranches:          buildMerkleBranches([][]byte{txID[:]}),
	}
	c := &sv2Conn{
		id:          "sv2-standard-assembly-pass",
		conn:        conn,
		rpc:         rpc,
		cfg:         Config{PoolFeePercent: 0},
		workerName:  "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-asm-pass",
		extranonce1: []byte{0x00, 0x00, 0x00, 0x01},
		difficulty:  1,
		channelID:   1,
		channelType: sv2ChannelTypeStandard,
	}
	if err := c.sendJob(job); err != nil {
		t.Fatalf("sendJob: %v", err)
	}
	jobAny, ok := c.activeJobs.Load(uint32(1))
	if !ok {
		t.Fatal("active job missing")
	}
	info := jobAny.(*sv2JobInfo)
	header80, err := buildBlockHeaderFromHex(
		int32(job.Template.Version),
		job.Template.Previous,
		info.standardMerkleRoot[:],
		fmt.Sprintf("%08x", uint32(job.Template.CurTime)),
		job.Template.Bits,
		fmt.Sprintf("%08x", uint32(2)),
	)
	if err != nil {
		t.Fatalf("build header: %v", err)
	}
	submit := &sv2SubmitSharesStandard{
		ChannelID:      1,
		SequenceNumber: 21,
		JobID:          1,
		Nonce:          2,
		NTime:          uint32(job.Template.CurTime),
		Version:        uint32(job.Template.Version),
	}
	c.handleSubmit(submit, nil, header80)
	if got := atomic.LoadInt32(&rpc.calls); got != 1 {
		t.Fatalf("submitblock calls = %d, want 1", got)
	}
	if len(rpc.blocks) != 1 {
		t.Fatalf("submitted blocks = %d, want 1", len(rpc.blocks))
	}
	submittedHeader := decodeHeader80FromBlockHex(t, rpc.blocks[0])
	if !bytes.Equal(submittedHeader, header80) {
		t.Fatalf("submitted header mismatch: got %x want %x", submittedHeader, header80)
	}
	coinbaseTxID := doubleSHA256Array(info.standardCoinbaseTx)
	recomputed, ok := sv2ComputeMerkleRootFromTxIDs([][32]byte{coinbaseTxID, txID})
	if !ok {
		t.Fatal("recompute merkle root failed")
	}
	if !bytes.Equal(recomputed[:], submittedHeader[36:68]) {
		t.Fatalf("recomputed merkle %x != header merkle %x", recomputed, submittedHeader[36:68])
	}
}

func TestSV2StandardSubmitblockAssemblyMerkleMismatchRejectedBeforeRPC(t *testing.T) {
	rpc := &sv2SubmitCaptureRPC{}
	conn := &sv2FrameCaptureConn{}
	txHex := "010000000100000000000000000000000000000000000000000000000000000000000000000000000000ffffffff010100000000000000015100000000"
	job := &Job{
		JobID:                   "sv2-std-assembly-mismatch",
		Template:                GetBlockTemplateResult{Height: 105, Version: 0x20000000, CurTime: 1700000005, Mintime: 1700000005, Bits: "1d00ffff", Previous: strings.Repeat("0", 64), CoinbaseValue: 50 * 1e8},
		Target:                  maxUint256ForStandardTest(t),
		Extranonce2Size:         4,
		TemplateExtraNonce2Size: 4,
		PayoutScript:            []byte{0x51},
		CoinbaseValue:           50 * 1e8,
		CoinbaseMsg:             "std-assembly-mismatch",
		ScriptTime:              0,
		Transactions:            []GBTTransaction{{Data: txHex}},
		MerkleBranches:          nil,
	}
	c := &sv2Conn{
		id:          "sv2-standard-assembly-mismatch",
		conn:        conn,
		rpc:         rpc,
		cfg:         Config{PoolFeePercent: 0},
		workerName:  "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh.worker-asm-mismatch",
		extranonce1: []byte{0x00, 0x00, 0x00, 0x01},
		difficulty:  1,
		channelID:   1,
		channelType: sv2ChannelTypeStandard,
	}
	if err := c.sendJob(job); err != nil {
		t.Fatalf("sendJob: %v", err)
	}
	jobAny, ok := c.activeJobs.Load(uint32(1))
	if !ok {
		t.Fatal("active job missing")
	}
	info := jobAny.(*sv2JobInfo)
	header80, err := buildBlockHeaderFromHex(
		int32(job.Template.Version),
		job.Template.Previous,
		info.standardMerkleRoot[:],
		fmt.Sprintf("%08x", uint32(job.Template.CurTime)),
		job.Template.Bits,
		fmt.Sprintf("%08x", uint32(3)),
	)
	if err != nil {
		t.Fatalf("build header: %v", err)
	}
	submit := &sv2SubmitSharesStandard{
		ChannelID:      1,
		SequenceNumber: 22,
		JobID:          1,
		Nonce:          3,
		NTime:          uint32(job.Template.CurTime),
		Version:        uint32(job.Template.Version),
	}
	c.handleSubmit(submit, nil, header80)
	if got := atomic.LoadInt32(&rpc.calls); got != 0 {
		t.Fatalf("submitblock calls = %d, want 0", got)
	}
	if len(conn.frames) == 0 {
		t.Fatal("expected share error frame")
	}
	last := conn.frames[len(conn.frames)-1]
	if last.msgType != sv2MsgSubmitSharesError {
		t.Fatalf("last msgType = 0x%02x, want 0x%02x", last.msgType, sv2MsgSubmitSharesError)
	}
	var errMsg sv2SubmitSharesError
	if err := errMsg.decode(last.payload); err != nil {
		t.Fatalf("decode submit error: %v", err)
	}
	if errMsg.ErrorCode != "submitblock-assembly-merkle-mismatch" {
		t.Fatalf("error code = %q, want submitblock-assembly-merkle-mismatch", errMsg.ErrorCode)
	}
}
