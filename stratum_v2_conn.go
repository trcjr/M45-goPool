package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type sv2Conn struct {
	id                 string
	conn               net.Conn
	reader             *bufio.Reader
	jobMgr             *JobManager
	rpc                rpcCaller
	cfg                Config
	metrics            *PoolMetrics
	accounting         *AccountStore
	workerLists        *workerListStore
	workerRegistry     *workerConnectionRegistry
	sv2Registry        *sv2WorkerRegistry
	extranonce1        []byte
	extranonce1Hex     string
	jobCh              chan *Job
	channelID          uint32
	workerName         string
	difficulty         float64
	activeTarget       [32]byte
	activeJobs         sync.Map // uint32 jobID → *sv2JobInfo
	nextJobID          uint32
	sequenceAck        uint32
	cleanupOnce        sync.Once
	connectedAt        time.Time
	isTLS              bool
	channelType        string
	writeMu            sync.Mutex
	isExtended         bool
	extranonceSize     uint16
	vardiff            VarDiffConfig
	minerMaxTarget     [32]byte
	minerMinDiff       float64
	hasMinerMaxTarget  bool
	previousDifficulty float64
	lastDiffChangeSV2  time.Time
	lastRetargetAt     time.Time
	shareWinStart      time.Time
	shareWinAccept     int
	jobOrder           []uint32
	shareCache         map[uint32]*duplicateShareSet

	statsMu                 sync.Mutex
	stats                   MinerStats
	lastShareHash           string
	lastShareAccepted       bool
	lastShareDifficulty     float64
	openNominalHashrate     float64
	lastRejectReason        string
	rollingHashrateControl  float64
	rollingHashrateValue    float64
	lastHashrateUpdate      time.Time
	hashrateSampleCount     int
	hashrateAccumulatedDiff float64
	initialEMAWindowDone    bool
	connectionSeq           uint64
	minerType               string
	minerClientName         string
	minerClientVersion      string
	savedWorkerTracked      bool
	savedWorkerBestDiff     float64
}

const (
	sv2ChannelTypeStandard = "standard"
	sv2ChannelTypeExtended = "extended"
)

type sv2SubmitWork struct {
	share      sv2SubmitSharesStandard
	extranonce []byte
	minerHeader80 []byte
}

type sv2SubmitVariant struct {
	name            string
	en1             []byte
	en2             []byte
	extranonce2Size int
	templateEx2Size int
}

type sv2MerkleTrace struct {
	CoinbaseTxIDWireLE       string
	CoinbaseTxIDDisplayBE    string
	MerkleBranchesHex        []string
	MerkleLayerHashesWireLE  []string
	MerkleLayerHashesBE      []string
	MerkleRootWireLE         string
	MerkleRootDisplayBE      string
}

func buildSV2MerkleTrace(coinbaseTxID []byte, branches []string) (sv2MerkleTrace, bool) {
	trace := sv2MerkleTrace{}
	if len(coinbaseTxID) != 32 {
		return trace, false
	}
	trace.CoinbaseTxIDWireLE = hex.EncodeToString(coinbaseTxID)
	trace.CoinbaseTxIDDisplayBE = hex.EncodeToString(reverseBytes(coinbaseTxID))
	trace.MerkleBranchesHex = append([]string(nil), branches...)

	var root [32]byte
	copy(root[:], coinbaseTxID)
	trace.MerkleLayerHashesWireLE = append(trace.MerkleLayerHashesWireLE, hex.EncodeToString(root[:]))
	trace.MerkleLayerHashesBE = append(trace.MerkleLayerHashesBE, hex.EncodeToString(reverseBytes(root[:])))

	var branch [32]byte
	var concat [64]byte
	for _, b := range branches {
		if err := decodeHexToFixedBytes(branch[:], b); err != nil {
			return trace, false
		}
		copy(concat[:32], root[:])
		copy(concat[32:], branch[:])
		root = doubleSHA256Array(concat[:])
		trace.MerkleLayerHashesWireLE = append(trace.MerkleLayerHashesWireLE, hex.EncodeToString(root[:]))
		trace.MerkleLayerHashesBE = append(trace.MerkleLayerHashesBE, hex.EncodeToString(reverseBytes(root[:])))
	}

	trace.MerkleRootWireLE = hex.EncodeToString(root[:])
	trace.MerkleRootDisplayBE = hex.EncodeToString(reverseBytes(root[:]))
	return trace, true
}

func txidFromSerializedTx(raw []byte) ([32]byte, error) {
	var zero [32]byte
	_, txid, consumed, err := parseSerializedTxForSubmitBlock(raw)
	if err != nil {
		return zero, err
	}
	if consumed != len(raw) {
		return zero, fmt.Errorf("tx has trailing bytes: consumed=%d total=%d", consumed, len(raw))
	}
	return txid, nil
}

type SV2TraceContext struct {
	Conn       string
	Chan       string
	Job        string
	Tmpl       string
	Seq        string
	Worker     string
	Remote     string
	Ver        string
	NTime      string
	Nonce      string
	NBits      string
	Prev       string
	ExN        string
	ExNLen     string
	CoinbaseTxID string
	Merkle     string
	Hdr80      string
	Hash       string
	SDiff      string
	TDiff      string
	NDiff      string
	ShareTarget string
	NetTarget  string
	Result     string
	Reason     string
	Mode       string
	JobType    string
}

func sv2TraceValue(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "-"
	}
	v = strings.ReplaceAll(v, "\n", "_")
	v = strings.ReplaceAll(v, "\r", "_")
	v = strings.ReplaceAll(v, "\t", "_")
	v = strings.ReplaceAll(v, " ", "_")
	if v == "" {
		return "-"
	}
	return v
}

func BuildSV2Trace(ctx *SV2TraceContext) string {
	if ctx == nil {
		ctx = &SV2TraceContext{}
	}
	return fmt.Sprintf(
		"trace=SV2_TRACE conn=%s chan=%s job=%s tmpl=%s seq=%s worker=%s remote=%s ver=%s ntime=%s nonce=%s nbits=%s prev=%s exn=%s exnlen=%s coinbase_txid=%s merkle=%s hdr80=%s hash=%s sdiff=%s tdiff=%s ndiff=%s share_target=%s net_target=%s result=%s reason=%s mode=%s jobtype=%s",
		sv2TraceValue(ctx.Conn),
		sv2TraceValue(ctx.Chan),
		sv2TraceValue(ctx.Job),
		sv2TraceValue(ctx.Tmpl),
		sv2TraceValue(ctx.Seq),
		sv2TraceValue(ctx.Worker),
		sv2TraceValue(ctx.Remote),
		sv2TraceValue(ctx.Ver),
		sv2TraceValue(ctx.NTime),
		sv2TraceValue(ctx.Nonce),
		sv2TraceValue(ctx.NBits),
		sv2TraceValue(ctx.Prev),
		sv2TraceValue(ctx.ExN),
		sv2TraceValue(ctx.ExNLen),
		sv2TraceValue(ctx.CoinbaseTxID),
		sv2TraceValue(ctx.Merkle),
		sv2TraceValue(ctx.Hdr80),
		sv2TraceValue(ctx.Hash),
		sv2TraceValue(ctx.SDiff),
		sv2TraceValue(ctx.TDiff),
		sv2TraceValue(ctx.NDiff),
		sv2TraceValue(ctx.ShareTarget),
		sv2TraceValue(ctx.NetTarget),
		sv2TraceValue(ctx.Result),
		sv2TraceValue(ctx.Reason),
		sv2TraceValue(ctx.Mode),
		sv2TraceValue(ctx.JobType),
	)
}

func sv2FormatFloat(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "-"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func sv2SubmittedPrevHashFromHeader80(minerHeader80 []byte) string {
	if len(minerHeader80) != 80 {
		return "-"
	}
	var prevLE [32]byte
	copy(prevLE[:], minerHeader80[4:36])
	var prevBE [32]byte
	for i := 0; i < 32; i++ {
		prevBE[i] = prevLE[31-i]
	}
	return hex.EncodeToString(prevBE[:])
}

func (c *sv2Conn) latestActiveSV2Job() (uint32, *sv2JobInfo, bool) {
	for i := len(c.jobOrder) - 1; i >= 0; i-- {
		id := c.jobOrder[i]
		if raw, ok := c.activeJobs.Load(id); ok {
			if info, castOK := raw.(*sv2JobInfo); castOK && info != nil {
				return id, info, true
			}
		}
	}
	return 0, nil, false
}

func (c *sv2Conn) logSV2StaleOrPausedWork(submit *sv2SubmitSharesStandard, submittedJob *Job, minerHeader80 []byte, reason string) {
	if submit == nil {
		return
	}
	now := time.Now()
	latestJobID, latestInfo, hasLatest := c.latestActiveSV2Job()
	currentJob := "-"
	latestPrevHash := "-"
	if hasLatest {
		currentJob = fmt.Sprintf("%d", latestJobID)
		if latestInfo != nil && latestInfo.job != nil {
			latestPrevHash = strings.TrimSpace(latestInfo.job.Template.Previous)
			if latestPrevHash == "" {
				latestPrevHash = "-"
			}
		}
	}
	templateJobID := "-"
	jobAgeMs := int64(-1)
	if submittedJob != nil {
		templateJobID = strings.TrimSpace(submittedJob.JobID)
		if templateJobID == "" {
			templateJobID = "-"
		}
		if !submittedJob.CreatedAt.IsZero() {
			jobAgeMs = now.Sub(submittedJob.CreatedAt).Milliseconds()
		}
	}
	isPaused := false
	isStopping := false
	healthReason := ""
	if c.jobMgr != nil {
		hs := stratumHealthStatus(c.jobMgr, now)
		isPaused = !hs.Healthy
		healthReason = strings.TrimSpace(hs.Reason)
	}
	traceCtx := c.newSV2TraceCtx()
	traceCtx.Chan = fmt.Sprintf("%d", submit.ChannelID)
	traceCtx.Job = fmt.Sprintf("%d", submit.JobID)
	traceCtx.Seq = fmt.Sprintf("%d", submit.SequenceNumber)
	traceCtx.Tmpl = templateJobID
	traceCtx.Ver = fmt.Sprintf("%08x", submit.Version)
	traceCtx.NTime = fmt.Sprintf("%08x", submit.NTime)
	traceCtx.Nonce = fmt.Sprintf("%08x", submit.Nonce)
	traceCtx.Result = "rejected"
	traceCtx.Reason = reason
	trace := BuildSV2Trace(traceCtx)
	logger.Warn("SV2_STALE_OR_PAUSED_WORK", "component", "stratum", "kind", "sv2_stale_or_paused",
		"remote", c.id,
		"sv2_trace", trace,
		"channel", submit.ChannelID,
		"submitted_job", submit.JobID,
		"submitted_seq", submit.SequenceNumber,
		"current_job", currentJob,
		"template_job", templateJobID,
		"is_paused", isPaused,
		"is_stopping", isStopping,
		"job_age_ms", jobAgeMs,
		"latest_prevhash", latestPrevHash,
		"submitted_prevhash", sv2SubmittedPrevHashFromHeader80(minerHeader80),
		"channel_type", c.channelType,
		"health_reason", healthReason,
		"reason", reason)
}

func (c *sv2Conn) newSV2TraceCtx() *SV2TraceContext {
	ctx := &SV2TraceContext{}
	if c == nil {
		return ctx
	}
	ctx.Conn = c.id
	ctx.Remote = c.id
	ctx.Worker = c.workerName
	ctx.Chan = fmt.Sprintf("%d", c.channelID)
	ctx.ExN = c.extranonce1Hex
	return ctx
}

func (c *sv2Conn) newSV2SubmitTraceCtx(submit *sv2SubmitSharesStandard, info *sv2JobInfo) *SV2TraceContext {
	ctx := c.newSV2TraceCtx()
	if submit != nil {
		ctx.Chan = fmt.Sprintf("%d", submit.ChannelID)
		ctx.Job = fmt.Sprintf("%d", submit.JobID)
		ctx.Seq = fmt.Sprintf("%d", submit.SequenceNumber)
		ctx.Ver = fmt.Sprintf("%08x", submit.Version)
		ctx.NTime = fmt.Sprintf("%08x", submit.NTime)
		ctx.Nonce = fmt.Sprintf("%08x", submit.Nonce)
	}
	if info != nil {
		ctx.JobType = info.jobType
		ctx.Mode = info.channelType
		if info.job != nil {
			ctx.Tmpl = info.job.JobID
			ctx.NBits = info.job.Template.Bits
			ctx.Prev = info.job.Template.Previous
		}
	}
	return ctx
}

func sv2CoinbaseScriptSigLengthFromCoinb1(coinb1 []byte) (uint64, error) {
	if len(coinb1) < 4 {
		return 0, fmt.Errorf("coinb1 too short")
	}
	idx := 4 // version
	vinCount, consumed, err := readVarInt(coinb1[idx:])
	if err != nil {
		return 0, fmt.Errorf("coinb1 inputs count: %w", err)
	}
	if vinCount < 1 {
		return 0, fmt.Errorf("coinb1 inputs count is zero")
	}
	idx += consumed
	if idx+36 > len(coinb1) {
		return 0, fmt.Errorf("coinb1 prevout truncated")
	}
	idx += 36
	scriptLen, _, err := readVarInt(coinb1[idx:])
	if err != nil {
		return 0, fmt.Errorf("coinb1 scriptSig len: %w", err)
	}
	return scriptLen, nil
}

func sv2DescribeSerializedTx(raw []byte) (consumed int, inputCount uint64, outputCount uint64, scriptSigLen uint64, err error) {
	if len(raw) < 10 {
		return 0, 0, 0, 0, fmt.Errorf("tx too short: %d bytes", len(raw))
	}

	idx := 4
	hasWitness := len(raw) > idx+1 && raw[idx] == 0x00 && raw[idx+1] != 0x00
	if hasWitness {
		idx += 2
	}

	vinCount, used, err := readVarInt(raw[idx:])
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("inputs count: %w", err)
	}
	idx += used
	inputCount = vinCount

	for inIdx := range vinCount {
		if idx+36 > len(raw) {
			return 0, inputCount, 0, 0, fmt.Errorf("input %d truncated", inIdx)
		}
		idx += 36
		inScriptLen, n, err := readVarInt(raw[idx:])
		if err != nil {
			return 0, inputCount, 0, 0, fmt.Errorf("input %d script len: %w", inIdx, err)
		}
		idx += n
		if inIdx == 0 {
			scriptSigLen = inScriptLen
		}
		if idx+int(inScriptLen)+4 > len(raw) {
			return 0, inputCount, 0, scriptSigLen, fmt.Errorf("input %d script truncated", inIdx)
		}
		idx += int(inScriptLen) + 4
	}

	voutCount, used, err := readVarInt(raw[idx:])
	if err != nil {
		return 0, inputCount, 0, scriptSigLen, fmt.Errorf("outputs count: %w", err)
	}
	idx += used
	outputCount = voutCount

	for outIdx := range voutCount {
		if idx+8 > len(raw) {
			return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("output %d truncated", outIdx)
		}
		idx += 8
		pkLen, n, err := readVarInt(raw[idx:])
		if err != nil {
			return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("output %d script len: %w", outIdx, err)
		}
		idx += n
		if idx+int(pkLen) > len(raw) {
			return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("output %d script truncated", outIdx)
		}
		idx += int(pkLen)
	}

	if hasWitness {
		for inIdx := range vinCount {
			itemCount, n, err := readVarInt(raw[idx:])
			if err != nil {
				return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("input %d witness count: %w", inIdx, err)
			}
			idx += n
			for itemIdx := range itemCount {
				itemLen, m, err := readVarInt(raw[idx:])
				if err != nil {
					return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("input %d witness %d len: %w", inIdx, itemIdx, err)
				}
				idx += m
				if idx+int(itemLen) > len(raw) {
					return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("input %d witness %d truncated", inIdx, itemIdx)
				}
				idx += int(itemLen)
			}
		}
	}

	if idx+4 > len(raw) {
		return 0, inputCount, outputCount, scriptSigLen, fmt.Errorf("locktime truncated")
	}
	idx += 4

	return idx, inputCount, outputCount, scriptSigLen, nil
}

func diffHeaderHexByteOffsets(aHex, bHex string, maxOffsets int) []int {
	if maxOffsets <= 0 {
		maxOffsets = 16
	}
	if len(aHex) != len(bHex) {
		return []int{-1}
	}
	offsets := make([]int, 0, maxOffsets)
	for i := 0; i+1 < len(aHex); i += 2 {
		if aHex[i] != bHex[i] || aHex[i+1] != bHex[i+1] {
			offsets = append(offsets, i/2)
			if len(offsets) >= maxOffsets {
				break
			}
		}
	}
	return offsets
}

func sv2HeaderHasMode(descriptors []string, mode string) bool {
	prefix := mode + "|"
	for _, d := range descriptors {
		if strings.HasPrefix(d, prefix) {
			return true
		}
	}
	return false
}

func sv2ModeIsCanonicalAuthoritative(mode, headerHex string, candidateHeaderModes map[string][]string) bool {
	switch mode {
	case "standard", "standard_direct", "tail_only":
		return true
	case "prefix_plus_tail":
		return sv2HeaderHasMode(candidateHeaderModes[headerHex], "tail_only")
	default:
		return false
	}
}

func sv2ParseSubmitHeaderHint(trailing []byte) ([]byte, string, bool) {
	if len(trailing) == 0 {
		return nil, "", false
	}
	if len(trailing) == 80 {
		h := make([]byte, 80)
		copy(h, trailing)
		return h, "raw80", true
	}
	if len(trailing) == 160 {
		h := make([]byte, 80)
		if _, err := hex.Decode(h, trailing); err == nil {
			return h, "hex160", true
		}
	}
	return nil, "", false
}

func sv2DiffIsMerkleRootOnly(offsets []int) bool {
	if len(offsets) == 0 {
		return false
	}
	for _, off := range offsets {
		if off < 36 || off > 67 {
			return false
		}
	}
	return true
}

func sv2ClassifyHeaderMismatch(diffOffsets []int) string {
	if len(diffOffsets) == 0 {
		return "header-mismatch"
	}
	if sv2DiffIsMerkleRootOnly(diffOffsets) {
		return "merkle-only-mismatch"
	}
	for _, off := range diffOffsets {
		if off < 36 || off > 67 {
			return "job-data-mismatch"
		}
	}
	return "header-mismatch"
}

func sv2ComputeMerkleRootFromTxIDs(txids [][32]byte) ([32]byte, bool) {
	var zero [32]byte
	if len(txids) == 0 {
		return zero, false
	}
	layer := make([][32]byte, len(txids))
	copy(layer, txids)
	for len(layer) > 1 {
		if len(layer)%2 == 1 {
			layer = append(layer, layer[len(layer)-1])
		}
		next := make([][32]byte, 0, len(layer)/2)
		for i := 0; i < len(layer); i += 2 {
			var pair [64]byte
			copy(pair[:32], layer[i][:])
			copy(pair[32:], layer[i+1][:])
			next = append(next, doubleSHA256Array(pair[:]))
		}
		layer = next
	}
	return layer[0], true
}

func NewSV2Conn(conn net.Conn, jobMgr *JobManager, rpc rpcCaller, cfg Config, metrics *PoolMetrics, accounting *AccountStore, workerRegistry *workerConnectionRegistry, sv2Registry *sv2WorkerRegistry, workerLists *workerListStore, isTLS bool) *sv2Conn {
	en1 := jobMgr.NextExtranonce1()
	vdiff := buildVarDiffConfig(cfg)
	diff := defaultMinDifficulty
	if cfg.DefaultDifficulty > 0 {
		diff = cfg.DefaultDifficulty
	} else if cfg.MinDifficulty > 0 {
		diff = cfg.MinDifficulty
	}
	if diff <= 0 {
		diff = 1.0
	}
	diff = clampDifficultyToRange(diff, vdiff.MinDiff, vdiff.MaxDiff)
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
		sv2Registry:    sv2Registry,
		extranonce1:    en1,
		extranonce1Hex: hex.EncodeToString(en1),
		channelID:      1,
		difficulty:     diff,
		connectedAt:    time.Now(),
		isTLS:          isTLS,
		channelType:    sv2ChannelTypeStandard,
		vardiff:        vdiff,
		jobOrder:       make([]uint32, 0, max(cfg.MaxRecentJobs, defaultRecentJobs)),
		shareCache:     make(map[uint32]*duplicateShareSet, max(cfg.MaxRecentJobs, defaultRecentJobs)),
	}
}

func (c *sv2Conn) assignConnectionSeq() {
	if atomic.LoadUint64(&c.connectionSeq) != 0 {
		return
	}
	id := atomic.AddUint64(&nextConnectionID, 1)
	atomic.StoreUint64(&c.connectionSeq, id)
}

func (c *sv2Conn) connectionIDString() string {
	seq := atomic.LoadUint64(&c.connectionSeq)
	if seq == 0 {
		return ""
	}
	return encodeBase58Uint64(seq - 1)
}

func (c *sv2Conn) workerHash() string {
	if c == nil {
		return ""
	}
	c.statsMu.Lock()
	hash := strings.TrimSpace(c.stats.WorkerSHA256)
	worker := c.stats.Worker
	c.statsMu.Unlock()
	if hash != "" {
		return hash
	}
	if worker == "" {
		worker = strings.TrimSpace(c.workerName)
	}
	return workerNameHash(worker)
}

func (c *sv2Conn) walletHash() string {
	if c == nil {
		return ""
	}
	worker := strings.TrimSpace(c.workerName)
	if worker == "" {
		c.statsMu.Lock()
		worker = strings.TrimSpace(c.stats.Worker)
		c.statsMu.Unlock()
	}
	return workerNameHash(workerBaseAddress(worker))
}

func (c *sv2Conn) registerWorker() {
	if c == nil || c.sv2Registry == nil {
		return
	}
	hash := c.workerHash()
	if hash == "" {
		return
	}
	c.sv2Registry.add(c)
}

func (c *sv2Conn) unregisterWorker() {
	if c == nil || c.sv2Registry == nil {
		return
	}
	c.sv2Registry.remove(c)
	c.savedWorkerTracked = false
	c.savedWorkerBestDiff = 0
}

func (c *sv2Conn) syncSavedWorkerState(hash string) {
	if c == nil {
		return
	}
	c.savedWorkerTracked = false
	c.savedWorkerBestDiff = 0
	if c.workerLists == nil {
		return
	}
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return
	}
	best, ok, err := c.workerLists.BestDifficultyForHash(hash)
	if err != nil {
		logger.Warn("sv2 saved worker best difficulty lookup failed", "component", "stratum", "kind", "sv2", "error", err, "hash", hash)
		return
	}
	c.savedWorkerBestDiff = best
	c.savedWorkerTracked = ok
}

func (c *sv2Conn) maybeUpdateSavedWorkerBestDiff(diff float64) {
	if c == nil || c.workerLists == nil {
		return
	}
	hash := c.workerHash()
	if hash == "" {
		return
	}
	if !c.savedWorkerTracked {
		return
	}
	if diff <= c.savedWorkerBestDiff {
		return
	}
	if _, err := c.workerLists.UpdateSavedWorkerBestDifficulty(hash, diff); err != nil {
		logger.Warn("sv2 saved worker best difficulty update failed", "component", "stratum", "kind", "sv2", "error", err, "hash", hash)
		return
	}
	c.savedWorkerBestDiff = diff
}

func (c *sv2Conn) maybeUpdateSavedWorkerMinuteBestDiff(diff float64, now time.Time) {
	if c == nil || c.workerLists == nil || diff <= 0 {
		return
	}
	hash := c.workerHash()
	if hash == "" {
		return
	}
	if !c.savedWorkerTracked {
		return
	}
	c.workerLists.UpdateSavedWorkerMinuteBestDifficulty(hash, diff, now)
}

func (c *sv2Conn) ensureWindowLocked(now time.Time) {
	if c.stats.WindowStart.IsZero() {
		c.stats.WindowStart = now
		c.stats.WindowDifficulty = 0
		return
	}
	if !c.stats.LastShare.IsZero() && now.Sub(c.stats.LastShare) > statusWindowIdleReset {
		c.stats.WindowStart = now
		c.stats.WindowAccepted = 0
		c.stats.WindowSubmissions = 0
		c.stats.WindowDifficulty = 0
	}
}

func (c *sv2Conn) hashrateEMATau() time.Duration {
	tau := c.cfg.HashrateEMATauSeconds
	if tau <= 0 {
		tau = defaultHashrateEMATauSeconds
	}
	return time.Duration(tau * float64(time.Second))
}

func (c *sv2Conn) hashrateControlTau() time.Duration {
	displayTau := c.hashrateEMATau()
	controlTau := time.Duration(float64(displayTau) * hashrateControlTauFactor)
	if controlTau < hashrateControlTauMin {
		controlTau = hashrateControlTauMin
	}
	if controlTau > displayTau {
		controlTau = displayTau
	}
	return controlTau
}

func (c *sv2Conn) suggestedStartDiffFromNominalHashrate(nominalHashrate float64) (float64, bool) {
	if c == nil || nominalHashrate <= 0 || math.IsNaN(nominalHashrate) || math.IsInf(nominalHashrate, 0) {
		return 0, false
	}
	targetShares := c.vardiff.TargetSharesPerMin
	if targetShares <= 0 {
		targetShares = defaultVarDiffTargetSharesPerMin
	}
	if targetShares <= 0 {
		return 0, false
	}
	diff := (nominalHashrate * 60.0) / (hashPerShare * targetShares)
	if diff <= 0 || math.IsNaN(diff) || math.IsInf(diff, 0) {
		return 0, false
	}
	diff = c.clampDifficultyWithMinerLimits(diff)
	return diff, diff > 0
}

func (c *sv2Conn) capDifficultyForJob(requestedDiff float64, job *Job) float64 {
	networkDiff, ok, err := networkDifficultyFromJob(job)
	if !ok {
		if err != nil {
			logger.Warn("sv2 share difficulty cap skipped", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName,
				"channel_id", c.channelID,
				"requested_share_diff_bdiff", requestedDiff,
				"reason", "invalid-network-difficulty", "error", err)
		}
		return requestedDiff
	}
	effective, capped, networkValid := capShareDifficultyByNetwork(requestedDiff, networkDiff)
	if !networkValid {
		logger.Warn("sv2 share difficulty cap skipped", "component", "stratum", "kind", "sv2",
			"remote", c.id, "worker", c.workerName,
			"channel_id", c.channelID,
			"requested_share_diff_bdiff", requestedDiff,
			"reason", "invalid-network-difficulty")
		return requestedDiff
	}
	if capped {
		logger.Debug("share difficulty capped by network", "component", "stratum", "kind", "sv2",
			"remote", c.id, "worker", c.workerName,
			"channel_id", c.channelID,
			"requested_share_diff_bdiff", requestedDiff,
			"network_diff_bdiff", networkDiff,
			"effective_share_diff_bdiff", effective,
			"reason", "network-difficulty-cap")
	}
	return effective
}

func (c *sv2Conn) clampDifficultyWithMinerLimits(diff float64) float64 {
	diff = clampDifficultyToRange(diff, c.vardiff.MinDiff, c.vardiff.MaxDiff)
	if c != nil && c.hasMinerMaxTarget && c.minerMinDiff > 0 && diff < c.minerMinDiff {
		diff = c.minerMinDiff
		diff = clampDifficultyToRange(diff, c.vardiff.MinDiff, c.vardiff.MaxDiff)
	}
	return diff
}

func (c *sv2Conn) targetForDifficulty(diff float64) [32]byte {
	target := sv2TargetFromDifficulty(diff)
	if c != nil && c.hasMinerMaxTarget && sv2TargetGreaterThan(target, c.minerMaxTarget) {
		return c.minerMaxTarget
	}
	return target
}

func (c *sv2Conn) meetsPrevDiffGrace(shareDiff float64, now time.Time) bool {
	if c == nil || shareDiff <= 0 || now.IsZero() {
		return false
	}
	if c.lastDiffChangeSV2.IsZero() || now.Sub(c.lastDiffChangeSV2) > previousDiffGracePeriod {
		return false
	}
	if c.previousDifficulty <= 0 {
		return false
	}
	ratio := shareDiff / c.previousDifficulty
	return ratio >= 0.98
}

func (c *sv2Conn) setAssignedDiffOnActiveJobs(diff float64) {
	if c == nil || diff <= 0 {
		return
	}
	c.activeJobs.Range(func(_, value any) bool {
		info, ok := value.(*sv2JobInfo)
		if ok && info != nil {
			info.requestedDiff = diff
			info.assignedDiff = c.capDifficultyForJob(diff, info.job)
		}
		return true
	})
}

func (c *sv2Conn) updateHashrateLocked(targetDiff float64, shareTime time.Time) {
	if targetDiff <= 0 || shareTime.IsZero() {
		return
	}
	controlTauSeconds := c.hashrateControlTau().Seconds()
	displayTauSeconds := c.hashrateEMATau().Seconds()

	if c.lastHashrateUpdate.IsZero() {
		c.lastHashrateUpdate = shareTime
		c.hashrateSampleCount = 1
		c.hashrateAccumulatedDiff = targetDiff
		return
	}

	c.hashrateSampleCount++
	c.hashrateAccumulatedDiff += targetDiff
	elapsed := shareTime.Sub(c.lastHashrateUpdate).Seconds()
	if elapsed <= 0 {
		return
	}
	if !c.initialEMAWindowDone && elapsed < controlTauSeconds {
		return
	}

	sample := (c.hashrateAccumulatedDiff * hashPerShare) / elapsed
	alphaControl := 1 - math.Exp(-elapsed/controlTauSeconds)
	if alphaControl < 0 {
		alphaControl = 0
	}
	if alphaControl > 1 {
		alphaControl = 1
	}
	alphaDisplay := 1 - math.Exp(-elapsed/displayTauSeconds)
	if alphaDisplay < 0 {
		alphaDisplay = 0
	}
	if alphaDisplay > 1 {
		alphaDisplay = 1
	}

	if c.rollingHashrateControl <= 0 {
		c.rollingHashrateControl = sample
	} else {
		c.rollingHashrateControl = c.rollingHashrateControl + alphaControl*(sample-c.rollingHashrateControl)
	}
	if c.rollingHashrateValue <= 0 {
		c.rollingHashrateValue = sample
	} else {
		c.rollingHashrateValue = c.rollingHashrateValue + alphaDisplay*(sample-c.rollingHashrateValue)
	}
	c.initialEMAWindowDone = true
	c.lastHashrateUpdate = shareTime
	c.hashrateSampleCount = 0
	c.hashrateAccumulatedDiff = 0

	if c.metrics != nil {
		connSeq := atomic.LoadUint64(&c.connectionSeq)
		if connSeq != 0 {
			c.metrics.UpdateConnectionHashrate(connSeq, c.rollingHashrateValue)
		}
	}
}

func (c *sv2Conn) recordShare(worker string, accepted bool, creditedDiff float64, shareDiff float64, reason string, shareHash string, now time.Time) {
	c.statsMu.Lock()
	c.ensureWindowLocked(now)
	if worker == "" {
		worker = c.workerName
	}
	if worker != "" {
		if c.stats.Worker != worker {
			c.stats.Worker = worker
			c.stats.WorkerSHA256 = workerNameHash(worker)
		} else if c.stats.WorkerSHA256 == "" {
			c.stats.WorkerSHA256 = workerNameHash(worker)
		}
	}
	c.stats.WindowSubmissions++
	if accepted {
		c.stats.Accepted++
		c.stats.WindowAccepted++
		if creditedDiff >= 0 {
			c.stats.TotalDifficulty += creditedDiff
			c.stats.WindowDifficulty += creditedDiff
			c.updateHashrateLocked(creditedDiff, now)
		}
	} else {
		c.stats.Rejected++
		if reason != "" {
			c.lastRejectReason = reason
		}
	}
	c.stats.LastShare = now
	c.lastShareHash = shareHash
	c.lastShareAccepted = accepted
	c.lastShareDifficulty = shareDiff
	c.statsMu.Unlock()

	if c.metrics != nil {
		c.metrics.RecordShare(accepted, reason)
	}
}

func (c *sv2Conn) snapshotShareInfo(now time.Time) minerShareSnapshot {
	c.statsMu.Lock()
	stats := c.stats
	if !stats.WindowStart.IsZero() && !stats.LastShare.IsZero() && now.Sub(stats.LastShare) > statusWindowIdleReset {
		stats.WindowStart = now
		stats.WindowAccepted = 0
		stats.WindowSubmissions = 0
		stats.WindowDifficulty = 0
	}
	rolling := c.rollingHashrateValue
	lastShareHash := c.lastShareHash
	lastShareAccepted := c.lastShareAccepted
	lastShareDifficulty := c.lastShareDifficulty
	lastReject := c.lastRejectReason
	c.statsMu.Unlock()

	return minerShareSnapshot{
		Stats:                  stats,
		RollingHashrate:        rolling,
		RollingHashrateDisplay: rolling,
		LastShareHash:          lastShareHash,
		LastShareAccepted:      lastShareAccepted,
		LastShareDifficulty:    lastShareDifficulty,
		LastReject:             lastReject,
	}
}

func (c *sv2Conn) minerClientInfo() (minerType, name, version string) {
	c.statsMu.Lock()
	minerType = c.minerType
	name = c.minerClientName
	version = c.minerClientVersion
	c.statsMu.Unlock()
	return minerType, name, version
}

func (c *sv2Conn) Close(reason string) {
	if c == nil {
		return
	}
	if strings.TrimSpace(reason) != "" {
		logger.Info("sv2 connection closing", "component", "stratum", "kind", "sv2", "remote", c.id, "reason", reason)
	}
	c.cleanup()
}

func (c *sv2Conn) adminBan(reason string, duration time.Duration) {
	if c == nil {
		return
	}
	if duration <= 0 {
		duration = defaultBanInvalidSubmissionsDuration
	}
	if reason == "" {
		reason = "admin ban"
	}
	worker := strings.TrimSpace(c.workerName)
	if worker == "" {
		worker = strings.TrimSpace(c.stats.Worker)
	}
	if c.accounting != nil && worker != "" {
		c.accounting.MarkBan(worker, time.Now().Add(duration), reason)
	}
}

func (c *sv2Conn) cleanup() {
	c.cleanupOnce.Do(func() {
		c.unregisterWorker()
		if c.jobMgr != nil && c.jobCh != nil {
			c.jobMgr.Unsubscribe(c.jobCh)
		}
		if c.metrics != nil {
			if connSeq := atomic.LoadUint64(&c.connectionSeq); connSeq != 0 {
				c.metrics.RemoveConnectionHashrate(connSeq)
			}
		}
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *sv2Conn) writeFrame(msgType byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	logger.Debug("sv2 writeFrame", "component", "stratum", "kind", "sv2", "remote", c.id, "msgType", msgType, "payload_len", len(payload))
	if fw, ok := c.conn.(interface {
		WriteSV2Frame(msgType byte, payload []byte) error
	}); ok {
		return fw.WriteSV2Frame(msgType, payload)
	}
	return sv2WriteFrame(c.conn, msgType, payload)
}

func (c *sv2Conn) readFrame() (byte, []byte, error) {
	if fr, ok := c.conn.(interface{ ReadSV2Frame() (byte, []byte, error) }); ok {
		return fr.ReadSV2Frame()
	}
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
	logger.Debug("sv2 setup frame", "component", "stratum", "kind", "sv2", "remote", c.id, "msgType", msgType, "payload_len", len(payload))
	if msgType != sv2MsgSetupConnection {
		logger.Warn("sv2 expected SetupConnection", "component", "stratum", "kind", "sv2", "remote", c.id, "got", msgType)
		return
	}
	var setup sv2SetupConnection
	if err := setup.decode(payload); err != nil {
		logger.Warn("sv2 decode SetupConnection", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	logger.Debug("sv2 setup decoded", "component", "stratum", "kind", "sv2", "remote", c.id,
		"protocol", setup.Protocol, "min_version", setup.MinVersion, "max_version", setup.MaxVersion, "flags", setup.Flags)
	// Protocol 0x00 = Mining Protocol
	if setup.Protocol != 0x00 {
		errMsg := sv2SetupConnectionError{Flags: 0, ErrorCode: "unsupported-feature-flags"}
		_ = c.writeFrame(sv2MsgSetupConnectionError, errMsg.encode())
		logger.Debug("sv2 handle: unsupported protocol", "protocol", setup.Protocol)
		return
	}

	// 2. Send SetupConnectionSuccess
	success := sv2SetupConnectionSuccess{UsedVersion: 2, Flags: 0}
	if err := c.writeFrame(sv2MsgSetupConnectionSuccess, success.encode()); err != nil {
		logger.Warn("sv2 write SetupConnectionSuccess", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	logger.Debug("sv2 setup success sent", "component", "stratum", "kind", "sv2", "remote", c.id)

	// 3. Read OpenStandardMiningChannel or OpenExtendedMiningChannel
	msgType, payload, err = c.readFrame()
	if err != nil {
		logger.Warn("sv2 read open channel", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	logger.Debug("sv2 open-channel frame", "component", "stratum", "kind", "sv2", "remote", c.id, "msgType", msgType, "payload_len", len(payload))

	var requestID uint32
	var userIdentity string
	var responseMsg byte
	var nominalHashrate float64

	if msgType == sv2MsgOpenStandardMiningChannel {
		var openCh sv2OpenStandardMiningChannel
		if err := openCh.decode(payload); err != nil {
			logger.Warn("sv2 decode OpenStandardMiningChannel", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			return
		}
		logger.Debug("sv2 open-channel decoded", "component", "stratum", "kind", "sv2", "remote", c.id,
			"request_id", openCh.RequestID, "user", openCh.UserIdentity, "nominal_hashrate", openCh.NominalHashRate, "channel_type", "standard")
		requestID = openCh.RequestID
		userIdentity = openCh.UserIdentity
		nominalHashrate = float64(openCh.NominalHashRate)
		if !sv2TargetIsZero(openCh.MaxTarget) {
			c.minerMaxTarget = openCh.MaxTarget
			if minDiff, ok := sv2DifficultyFromTargetLE(openCh.MaxTarget); ok {
				c.minerMinDiff = minDiff
				c.hasMinerMaxTarget = true
			}
		}
		responseMsg = sv2MsgOpenStdMiningChannelSuccess
		c.isExtended = false
		c.channelType = sv2ChannelTypeStandard
		c.extranonceSize = uint16(len(c.extranonce1))
	} else if msgType == sv2MsgOpenExtendedMiningChannel {
		var openCh sv2OpenExtendedMiningChannel
		if err := openCh.decode(payload); err != nil {
			logger.Warn("sv2 decode OpenExtendedMiningChannel", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			return
		}
		ex2Size := c.cfg.Extranonce2Size
		if ex2Size <= 0 {
			ex2Size = defaultExtranonce2Size
		}
		// Extended channel extranonce_size is negotiated as the mutable downstream
		// span; downstream also receives ExtraNoncePrefix separately.
		negotiatedEx2Size := ex2Size
		if int(openCh.MinExtranonceSize) > negotiatedEx2Size {
			negotiatedEx2Size = int(openCh.MinExtranonceSize)
		}
		if negotiatedEx2Size > 32 {
			errMsg := sv2OpenStdMiningChannelError{RequestID: openCh.RequestID, ErrorCode: "unsupported-min-extranonce-size"}
			_ = c.writeFrame(sv2MsgOpenMiningChannelError, errMsg.encode())
			logger.Warn("sv2 unsupported min_extranonce_size", "component", "stratum", "kind", "sv2", "remote", c.id,
				"requested", openCh.MinExtranonceSize, "supported_max", 32)
			return
		}
		logger.Debug("sv2 open-channel decoded", "component", "stratum", "kind", "sv2", "remote", c.id,
			"request_id", openCh.RequestID, "user", openCh.UserIdentity, "nominal_hashrate", openCh.NominalHashRate, "channel_type", "extended")
		requestID = openCh.RequestID
		userIdentity = openCh.UserIdentity
		nominalHashrate = float64(openCh.NominalHashRate)
		if !sv2TargetIsZero(openCh.MaxTarget) {
			c.minerMaxTarget = openCh.MaxTarget
			if minDiff, ok := sv2DifficultyFromTargetLE(openCh.MaxTarget); ok {
				c.minerMinDiff = minDiff
				c.hasMinerMaxTarget = true
			}
		}
		responseMsg = sv2MsgOpenExtMiningChannelSuccess
		c.isExtended = true
		c.channelType = sv2ChannelTypeExtended
		c.extranonceSize = uint16(negotiatedEx2Size)
	} else {
		logger.Warn("sv2 expected OpenStandardMiningChannel or OpenExtendedMiningChannel", "component", "stratum", "kind", "sv2", "remote", c.id, "got", msgType)
		return
	}
	validatedWorker, errCode, err := c.validateWorkerIdentity(userIdentity)
	if err != nil {
		errMsg := sv2OpenStdMiningChannelError{RequestID: requestID, ErrorCode: errCode}
		_ = c.writeFrame(sv2MsgOpenMiningChannelError, errMsg.encode())
		logger.Warn("sv2 open-channel rejected", "component", "stratum", "kind", "sv2", "remote", c.id,
			"worker", userIdentity, "error_code", errCode, "error", err)
		return
	}
	c.workerName = validatedWorker
	c.openNominalHashrate = nominalHashrate
	c.assignConnectionSeq()
	c.registerWorker()
	c.syncSavedWorkerState(c.workerHash())
	c.statsMu.Lock()
	c.stats.Worker = validatedWorker
	c.stats.WorkerSHA256 = workerNameHash(validatedWorker)
	c.minerType = strings.TrimSpace(setup.Vendor)
	c.minerClientName = strings.TrimSpace(setup.Vendor)
	c.minerClientVersion = strings.TrimSpace(setup.Firmware)
	c.statsMu.Unlock()
	if startDiff, ok := c.suggestedStartDiffFromNominalHashrate(nominalHashrate); ok {
		old := c.difficulty
		c.difficulty = c.clampDifficultyWithMinerLimits(startDiff)
		if curJob := c.jobMgr.CurrentJob(); curJob != nil {
			c.difficulty = c.capDifficultyForJob(c.difficulty, curJob)
		}
		if math.Abs(startDiff-old) > 1e-6 {
			logger.Info("sv2 startup difficulty from nominal hashrate", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName,
				"nominal_hashrate_hs", nominalHashrate,
				"target_shares_per_min", c.vardiff.TargetSharesPerMin,
				"old_diff", old, "new_diff", c.difficulty)
		}
	}
	c.difficulty = c.clampDifficultyWithMinerLimits(c.difficulty)
	if curJob := c.jobMgr.CurrentJob(); curJob != nil {
		c.difficulty = c.capDifficultyForJob(c.difficulty, curJob)
	}

	target := c.targetForDifficulty(c.difficulty)
	c.activeTarget = target

	// 4. Send OpenMiningChannelSuccess (appropriate type based on request)
	if responseMsg == sv2MsgOpenStdMiningChannelSuccess {
		chanSuccess := sv2OpenStdMiningChannelSuccess{
			RequestID:        requestID,
			ChannelID:        c.channelID,
			Target:           target,
			ExtraNoncePrefix: c.extranonce1,
			GroupChannelID:   0,
		}
		if err := c.writeFrame(sv2MsgOpenStdMiningChannelSuccess, chanSuccess.encode()); err != nil {
			logger.Warn("sv2 write OpenStdMiningChannelSuccess", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			return
		}
	} else {
		chanSuccess := sv2OpenExtMiningChannelSuccess{
			RequestID:        requestID,
			ChannelID:        c.channelID,
			Target:           target,
			ExtranonceSize:   c.extranonceSize,
			ExtraNoncePrefix: c.extranonce1,
			GroupChannelID:   0,
		}
		if err := c.writeFrame(sv2MsgOpenExtMiningChannelSuccess, chanSuccess.encode()); err != nil {
			logger.Warn("sv2 write OpenExtMiningChannelSuccess", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			return
		}
	}
	logger.Debug("sv2 open-channel success sent", "component", "stratum", "kind", "sv2", "remote", c.id, "channel_id", c.channelID)

	logger.Info("sv2 channel opened", "component", "stratum", "kind", "sv2",
		"remote", c.id, "worker", c.workerName, "channel_id", c.channelID)
	if shareValidationDebugEnabled() {
		logger.Debug("share validation debug",
			"component", "stratum",
			"kind", "sv2_open_channel",
			"stratum_version", "sv2",
			"connection_id", c.id,
			"worker_name", c.workerName,
			"channel_id", c.channelID,
			"channel_type", c.channelType,
			"open_nominal_hash_rate", nominalHashrate,
			"open_max_target", func() string {
				if c.hasMinerMaxTarget {
					return hex.EncodeToString(c.minerMaxTarget[:])
				}
				return ""
			}(),
			"open_success_target", hex.EncodeToString(target[:]),
			"required_difficulty", c.difficulty,
		)
	}

	// 5. Subscribe to job feed and send current job
	c.jobCh = c.jobMgr.Subscribe()
	curJob := c.jobMgr.CurrentJob()
	if curJob != nil {
		if err := c.sendJob(curJob); err != nil {
			logger.Warn("sv2 send initial job", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
			return
		}
		logger.Debug("sv2 initial job sent", "component", "stratum", "kind", "sv2", "remote", c.id, "height", curJob.Template.Height)
	}

	// 6. Goroutine: read frames from miner
	submitCh := make(chan *sv2SubmitWork, 16)
	errCh := make(chan error, 2)

	go func() {
		for {
			mt, pay, err := c.readFrame()
			if err != nil {
				errCh <- err
				return
			}
			logger.Debug("sv2 miner frame", "component", "stratum", "kind", "sv2", "remote", c.id, "msgType", mt, "payload_len", len(pay))
			if mt == sv2MsgSubmitSharesStandard {
				if c.isExtended {
					logger.Warn("sv2 submit standard on extended channel", "component", "stratum", "kind", "sv2", "remote", c.id,
						"reason", "submit-message-channel-mismatch")
					continue
				}
				var submit sv2SubmitSharesStandard
				trailing, decErr := submit.decodeWithTrailing(pay)
				if decErr != nil {
					logger.Warn("sv2 decode submit", "component", "stratum", "kind", "sv2", "remote", c.id, "error", decErr)
					continue
				}
				headerHint, hintSource, hintOK := sv2ParseSubmitHeaderHint(trailing)
				if len(trailing) > 0 && !hintOK {
					logger.Warn("sv2 submit trailing payload ignored", "component", "stratum", "kind", "sv2", "remote", c.id,
						"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
						"trailing_len", len(trailing), "reason", "unsupported trailing format")
				}
				logger.Debug("sv2 submit decoded", "component", "stratum", "kind", "sv2", "remote", c.id,
					"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
					"sv2_trace", BuildSV2Trace(c.newSV2SubmitTraceCtx(&submit, nil)),
					"header_hint_present", hintOK, "header_hint_source", hintSource)
				submitCh <- &sv2SubmitWork{share: submit, minerHeader80: headerHint}
				continue
			}
			if mt == sv2MsgSubmitSharesExtended {
				if !c.isExtended {
					logger.Warn("sv2 submit extended on standard channel", "component", "stratum", "kind", "sv2", "remote", c.id,
						"reason", "submit-message-channel-mismatch")
					continue
				}
				var submit sv2SubmitSharesExtended
				trailing, decErr := submit.decodeWithTrailing(pay)
				if decErr != nil {
					logger.Warn("sv2 decode submit extended", "component", "stratum", "kind", "sv2", "remote", c.id, "error", decErr)
					continue
				}
				headerHint, hintSource, hintOK := sv2ParseSubmitHeaderHint(trailing)
				if len(trailing) > 0 && !hintOK {
					logger.Warn("sv2 submit extended trailing payload ignored", "component", "stratum", "kind", "sv2", "remote", c.id,
						"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
						"trailing_len", len(trailing), "reason", "unsupported trailing format")
				}
				logger.Debug("sv2 submit extended decoded", "component", "stratum", "kind", "sv2", "remote", c.id,
					"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID, "extranonce_len", len(submit.Extranonce),
					"sv2_trace", BuildSV2Trace(c.newSV2SubmitTraceCtx(&sv2SubmitSharesStandard{
						ChannelID:      submit.ChannelID,
						SequenceNumber: submit.SequenceNumber,
						JobID:          submit.JobID,
						Nonce:          submit.Nonce,
						NTime:          submit.NTime,
						Version:        submit.Version,
					}, nil)),
					"header_hint_present", hintOK, "header_hint_source", hintSource)
				submitCh <- &sv2SubmitWork{share: sv2SubmitSharesStandard{
					ChannelID:      submit.ChannelID,
					SequenceNumber: submit.SequenceNumber,
					JobID:          submit.JobID,
					Nonce:          submit.Nonce,
					NTime:          submit.NTime,
					Version:        submit.Version,
				}, extranonce: submit.Extranonce, minerHeader80: headerHint}
				continue
			}
			logger.Debug("sv2 ignored unsupported miner message", "component", "stratum", "kind", "sv2", "remote", c.id, "msgType", mt)
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
			logger.Debug("sv2 new job", "component", "stratum", "kind", "sv2", "remote", c.id, "height", job.Template.Height)
			if err := c.sendJob(job); err != nil {
				logger.Warn("sv2 send job", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
				return
			}

		case submit, ok := <-submitCh:
			if !ok {
				return
			}
			logger.Debug("sv2 submit queued", "component", "stratum", "kind", "sv2", "remote", c.id,
				"channel_id", submit.share.ChannelID, "seq", submit.share.SequenceNumber, "job_id", submit.share.JobID, "extranonce_len", len(submit.extranonce),
				"header_hint_present", len(submit.minerHeader80) == 80, "channel_type", c.channelType)
			c.handleSubmit(&submit.share, submit.extranonce, submit.minerHeader80)
		}
	}
}

func (c *sv2Conn) sendJob(job *Job) error {
	if job == nil {
		return nil
	}
	logger.Debug("sv2 sendJob build", "component", "stratum", "kind", "sv2", "remote", c.id, "height", job.Template.Height)
	// Build coinbase parts.
	// For extended channels, use the negotiated downstream mutable extranonce span
	// directly to avoid v1-style template padding bytes in CbPrefix.
	ex2Size := job.Extranonce2Size
	templateEx2Size := job.TemplateExtraNonce2Size
	if c.isExtended {
		// For extended channels, use the negotiated mutable downstream span.
		downstreamEx2 := int(c.extranonceSize)
		if downstreamEx2 > 0 {
			ex2Size = downstreamEx2
			templateEx2Size = downstreamEx2
		}
	}

	coinb1, coinb2, err := c.buildPayoutCoinbaseParts(job, ex2Size, templateEx2Size)
	if err != nil {
		logger.Error("sv2 sendJob: build coinbase error", "error", err)
		return fmt.Errorf("sv2 build coinbase: %w", err)
	}

	jobID := c.nextSV2JobID()
	requestedJobDiff := c.difficulty
	effectiveJobDiff := c.capDifficultyForJob(requestedJobDiff, job)
	assignedTarget := c.targetForDifficulty(effectiveJobDiff)
	info := &sv2JobInfo{
		job:                        job,
		coinb1:                     coinb1,
		coinb2:                     coinb2,
		requestedDiff:              requestedJobDiff,
		assignedDiff:               effectiveJobDiff,
		assignedTarget:             assignedTarget,
		channelType:                c.channelType,
		expectsMerkleReconstruction: c.isExtended,
	}
	c.activeJobs.Store(jobID, info)
	c.jobOrder = append(c.jobOrder, jobID)
	c.pruneActiveJobs()

	logger.Debug("sv2 sendJob new-mining-job", "component", "stratum", "kind", "sv2", "remote", c.id,
		"job_id", jobID, "merkle_branches", len(job.MerkleBranches), "coinb1_len", len(coinb1), "coinb2_len", len(coinb2),
		"sv2_trace", func() string {
			ctx := c.newSV2TraceCtx()
			ctx.Job = fmt.Sprintf("%d", jobID)
			ctx.Tmpl = job.JobID
			ctx.Ver = fmt.Sprintf("%08x", uint32(job.Template.Version))
			ctx.NBits = job.Template.Bits
			ctx.Prev = job.Template.Previous
			ctx.Mode = c.channelType
			ctx.JobType = "new_mining_job"
			return BuildSV2Trace(ctx)
		}(),
		"channel_type", c.channelType)

	coinb1Bytes, err := hex.DecodeString(coinb1)
	if err != nil {
		return fmt.Errorf("sv2 decode coinb1: %w", err)
	}
	coinb2Bytes, err := hex.DecodeString(coinb2)
	if err != nil {
		return fmt.Errorf("sv2 decode coinb2: %w", err)
	}

	if c.isExtended {
		info.jobType = "new_extended_mining_job"
		jobMsg := sv2BuildNewExtendedMiningJob(info, c.channelID, jobID)
		if err := c.writeFrame(sv2MsgNewExtendedMiningJob, jobMsg.encode()); err != nil {
			return err
		}
	} else {
		info.jobType = "new_mining_job"
		// coinb1 reserves len(extranonce1)+templateEx2Size bytes, and may already
		// include template padding when templateEx2Size > ex2Size. Insert only the
		// canonical fixed extranonce blob expected by the split: ex1 + ex2.
		fixedExtranonce := make([]byte, 0, len(c.extranonce1)+ex2Size)
		fixedExtranonce = append(fixedExtranonce, c.extranonce1...)
		if ex2Size > 0 {
			fixedExtranonce = append(fixedExtranonce, make([]byte, ex2Size)...)
		}
		info.standardExtranonce = append([]byte(nil), fixedExtranonce...)
		standardCoinbase := make([]byte, 0, len(coinb1Bytes)+len(fixedExtranonce)+len(coinb2Bytes))
		standardCoinbase = append(standardCoinbase, coinb1Bytes...)
		standardCoinbase = append(standardCoinbase, fixedExtranonce...)
		standardCoinbase = append(standardCoinbase, coinb2Bytes...)
		scriptSigLenBefore := uint64(0)
		scriptSigLenAfter := uint64(0)
		if declaredScriptSigLen, scriptLenErr := sv2CoinbaseScriptSigLengthFromCoinb1(coinb1Bytes); scriptLenErr == nil {
			scriptSigLenAfter = declaredScriptSigLen
			if declaredScriptSigLen >= uint64(len(fixedExtranonce)) {
				scriptSigLenBefore = declaredScriptSigLen - uint64(len(fixedExtranonce))
			}
		} else {
			logger.Warn("sv2 standard coinbase scriptSig length parse error", "component", "stratum", "kind", "sv2", "remote", c.id, "error", scriptLenErr)
		}
		parsedConsumed, parsedInputCount, parsedOutputCount, parsedScriptSigLen, parsedErr := sv2DescribeSerializedTx(standardCoinbase)
		logger.Debug("sv2 standard coinbase pre-parse", "component", "stratum", "kind", "sv2", "remote", c.id,
			"job_id", jobID,
			"coinb1_len", len(coinb1Bytes),
			"coinb2_len", len(coinb2Bytes),
			"extranonce_len", len(fixedExtranonce),
			"full_coinbase_len", len(standardCoinbase),
			"full_coinbase_hex", hex.EncodeToString(standardCoinbase),
			"parsed_tx_consumed_bytes", parsedConsumed,
			"parsed_tx_total_bytes", len(standardCoinbase),
			"parsed_input_count", parsedInputCount,
			"parsed_output_count", parsedOutputCount,
			"parsed_script_sig_len", parsedScriptSigLen,
			"script_sig_len_before_extranonce_insertion", scriptSigLenBefore,
			"script_sig_len_after_extranonce_insertion", scriptSigLenAfter,
			"parser_stop_offset", parsedConsumed,
			"parse_error", parsedErr)
		coinbaseTxID, err := txidFromSerializedTx(standardCoinbase)
		if err != nil {
			return fmt.Errorf("sv2 parse standard coinbase txid: %w", err)
		}
		merkleTrace, ok := buildSV2MerkleTrace(coinbaseTxID[:], job.MerkleBranches)
		if !ok {
			return fmt.Errorf("sv2 build standard merkle trace")
		}
		merkleRootWireLE, err := hex.DecodeString(merkleTrace.MerkleRootWireLE)
		if err != nil || len(merkleRootWireLE) != 32 {
			return fmt.Errorf("sv2 decode standard merkle root")
		}
		copy(info.standardMerkleRoot[:], merkleRootWireLE)
		info.standardCoinbaseTx = append([]byte(nil), standardCoinbase...)
		jobMsg := sv2BuildNewStandardMiningJob(job, c.channelID, jobID, info.standardMerkleRoot)
		if err := c.writeFrame(sv2MsgNewMiningJob, jobMsg.encode()); err != nil {
			return err
		}
	}

	prevHashMsg := sv2BuildSetNewPrevHash(job, c.channelID, jobID)
	nbitsHex := fmt.Sprintf("%08x", prevHashMsg.NBits)
	networkTargetHex := ""
	if nbitsTarget, err := targetFromBits(nbitsHex); err == nil {
		networkTargetBE := uint256BEFromBigInt(nbitsTarget)
		networkTargetHex = hex.EncodeToString(networkTargetBE[:])
	}
	logger.Debug("sv2 sendJob set-prevhash", "component", "stratum", "kind", "sv2", "remote", c.id,
		"job_id", jobID,
		"sv2_trace", func() string {
			ctx := c.newSV2TraceCtx()
			ctx.Job = fmt.Sprintf("%d", jobID)
			ctx.Tmpl = job.JobID
			ctx.Ver = fmt.Sprintf("%08x", uint32(job.Template.Version))
			ctx.NBits = nbitsHex
			ctx.Prev = job.Template.Previous
			ctx.Mode = c.channelType
			ctx.JobType = info.jobType
			ctx.NetTarget = networkTargetHex
			ctx.NDiff = sv2FormatFloat(difficultyFromBits(prevHashMsg.NBits))
			return BuildSV2Trace(ctx)
		}(),
		"height", job.Template.Height,
		"template_bits", job.Template.Bits,
		"nbits_u32", nbitsHex,
		"network_diff_bdiff", difficultyFromBits(prevHashMsg.NBits),
		"network_target_be", networkTargetHex)
	if err := c.writeFrame(sv2MsgSetNewPrevHash, prevHashMsg.encode()); err != nil {
		logger.Error("sv2 sendJob: write SetNewPrevHash error", "error", err)
		return err
	}

	traceTarget := info.assignedTarget
	traceTargetHex := hex.EncodeToString(traceTarget[:])
	logger.Debug("sv2 job trace", "component", "stratum", "kind", "sv2_job_trace",
		"remote", c.id,
		"worker", c.workerName,
		"channel_id", c.channelID,
		"channel_type", c.channelType,
		"job_type", info.jobType,
		"seq_job_id", jobID,
		"template_job_id", job.JobID,
		"height", job.Template.Height,
		"version", fmt.Sprintf("%08x", uint32(job.Template.Version)),
		"min_ntime", prevHashMsg.MinNTime,
		"nbits", fmt.Sprintf("%08x", prevHashMsg.NBits),
		"prev_hash", hex.EncodeToString(prevHashMsg.PrevHash[:]),
		"uncapped_requested_share_diff_bdiff", info.requestedDiff,
		"assigned_share_diff_bdiff", info.assignedDiff,
		"target_le_hex", traceTargetHex,
		"is_extended", c.isExtended,
		"extranonce1_hex", c.extranonce1Hex,
		"negotiated_extranonce_size", c.extranonceSize,
		"standard_inserted_extranonce_hex", hex.EncodeToString(info.standardExtranonce),
		"standard_inserted_extranonce_len", len(info.standardExtranonce),
		"standard_fixed_coinbase_len", len(info.standardCoinbaseTx))

	return nil
}

func (c *sv2Conn) pruneActiveJobs() {
	maxJobs := c.cfg.MaxRecentJobs
	if maxJobs <= 0 {
		maxJobs = defaultRecentJobs
	}
	if len(c.jobOrder) <= maxJobs {
		return
	}
	trim := len(c.jobOrder) - maxJobs
	for i := 0; i < trim; i++ {
		oldID := c.jobOrder[i]
		c.activeJobs.Delete(oldID)
		delete(c.shareCache, oldID)
	}
	c.jobOrder = append([]uint32(nil), c.jobOrder[trim:]...)
}

func (c *sv2Conn) validateWorkerIdentity(raw string) (string, string, error) {
	worker := strings.TrimSpace(raw)
	if worker == "" {
		return "", "invalid-user-identity", fmt.Errorf("worker name required")
	}
	if len(worker) > maxWorkerNameLen {
		return "", "invalid-user-identity", fmt.Errorf("worker name too long")
	}
	if c.accounting != nil {
		if view, ok := c.accounting.WorkerViewByName(worker); ok && view.Banned {
			reason := strings.TrimSpace(view.BanReason)
			if reason == "" {
				reason = "banned"
			}
			return "", "unauthorized-worker", fmt.Errorf("worker banned: %s", reason)
		}
		wallet := workerBaseAddress(worker)
		if wallet != "" && wallet != worker {
			if view, ok := c.accounting.WorkerViewByName(wallet); ok && view.Banned {
				reason := strings.TrimSpace(view.BanReason)
				if reason == "" {
					reason = "banned"
				}
				return "", "unauthorized-worker", fmt.Errorf("wallet banned: %s", reason)
			}
		}
	}
	wallet := workerBaseAddress(worker)
	if wallet == "" {
		return "", "invalid-user-identity", fmt.Errorf("invalid wallet-style worker")
	}
	if _, err := scriptForAddress(wallet, ChainParams()); err != nil {
		return "", "invalid-user-identity", fmt.Errorf("invalid wallet address: %w", err)
	}
	return worker, "", nil
}

func (c *sv2Conn) buildPayoutCoinbaseParts(job *Job, ex2Size int, templateEx2Size int) (string, string, error) {
	return c.buildPayoutCoinbasePartsWithScriptTime(job, ex2Size, templateEx2Size, job.ScriptTime)
}

func (c *sv2Conn) buildPayoutCoinbasePartsWithScriptTime(job *Job, ex2Size int, templateEx2Size int, scriptTime int64) (string, string, error) {
	if job == nil {
		return "", "", fmt.Errorf("nil job")
	}
	workerAddr := workerBaseAddress(c.workerName)
	if workerAddr == "" {
		return "", "", fmt.Errorf("invalid worker identity for payout: %q", c.workerName)
	}
	workerScript, err := scriptForAddress(workerAddr, ChainParams())
	if err != nil || len(workerScript) == 0 {
		if err == nil {
			err = fmt.Errorf("empty worker payout script")
		}
		return "", "", fmt.Errorf("derive worker payout script: %w", err)
	}

	if poolScript, workerScript, totalValue, feePercent, ok := c.dualPayoutParams(job, workerAddr, workerScript); ok {
		if job.OperatorDonationPercent > 0 && len(job.DonationScript) > 0 {
			return buildTriplePayoutCoinbaseParts(
				job.Template.Height,
				c.extranonce1,
				ex2Size,
				templateEx2Size,
				poolScript,
				job.DonationScript,
				workerScript,
				totalValue,
				feePercent,
				job.OperatorDonationPercent,
				job.WitnessCommitment,
				job.Template.CoinbaseAux.Flags,
				job.CoinbaseMsg,
				scriptTime,
			)
		}
		return buildDualPayoutCoinbaseParts(
			job.Template.Height,
			c.extranonce1,
			ex2Size,
			templateEx2Size,
			poolScript,
			workerScript,
			totalValue,
			feePercent,
			job.WitnessCommitment,
			job.Template.CoinbaseAux.Flags,
			job.CoinbaseMsg,
			scriptTime,
		)
	}

	payoutScript := c.singlePayoutScript(job, workerScript)
	if len(payoutScript) == 0 {
		return "", "", fmt.Errorf("missing payout script for worker %q", workerAddr)
	}
	return buildCoinbaseParts(
		job.Template.Height,
		c.extranonce1,
		ex2Size,
		templateEx2Size,
		payoutScript,
		job.CoinbaseValue,
		job.WitnessCommitment,
		job.Template.CoinbaseAux.Flags,
		job.CoinbaseMsg,
		scriptTime,
	)
}

func (c *sv2Conn) singlePayoutScript(job *Job, workerScript []byte) []byte {
	if job == nil || len(job.PayoutScript) == 0 {
		return nil
	}
	if c.cfg.PoolFeePercent > 0 {
		return job.PayoutScript
	}
	if len(workerScript) == 0 {
		return nil
	}
	return workerScript
}

func (c *sv2Conn) dualPayoutParams(job *Job, workerAddr string, workerScript []byte) (poolScript []byte, workerOutScript []byte, totalValue int64, feePercent float64, ok bool) {
	if job == nil || job.CoinbaseValue <= 0 || len(job.PayoutScript) == 0 {
		return nil, nil, 0, 0, false
	}
	if c.cfg.PoolFeePercent <= 0 {
		return nil, nil, 0, 0, false
	}
	if len(workerScript) == 0 {
		return nil, nil, 0, 0, false
	}
	if workerAddr != "" && strings.EqualFold(workerAddr, c.cfg.PayoutAddress) {
		return nil, nil, 0, 0, false
	}
	return job.PayoutScript, workerScript, job.CoinbaseValue, c.cfg.PoolFeePercent, true
}

func (c *sv2Conn) handleSubmit(submit *sv2SubmitSharesStandard, extranonce []byte, minerHeader80 []byte) {
	now := time.Now()
	submitTrace := BuildSV2Trace(c.newSV2SubmitTraceCtx(submit, nil))
	logger.Debug("sv2 submit", "component", "stratum", "kind", "sv2", "remote", c.id,
		"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
		"sv2_trace", submitTrace,
		"nonce", submit.Nonce, "ntime", submit.NTime, "esp_header80_present", len(minerHeader80) == 80,
		"channel_type", c.channelType)
	jobIDAny, ok := c.activeJobs.Load(submit.JobID)
	if !ok {
		logger.Warn("sv2 handleSubmit: job not found", "jobID", submit.JobID, "sv2_trace", submitTrace)
		c.logSV2StaleOrPausedWork(submit, nil, minerHeader80, "job-not-found")
		c.recordShare(c.workerName, false, 0, 0, "stale job", "", now)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "job-not-found")
		return
	}
	info := jobIDAny.(*sv2JobInfo)
	job := info.job
	submitTrace = BuildSV2Trace(c.newSV2SubmitTraceCtx(submit, info))
	if latestJobID, _, hasLatest := c.latestActiveSV2Job(); hasLatest && latestJobID != submit.JobID {
		c.logSV2StaleOrPausedWork(submit, job, minerHeader80, "stale-job-after-new-job-issued")
	}
	if hs := stratumHealthStatus(c.jobMgr, now); !hs.Healthy {
		reason := strings.TrimSpace(hs.Reason)
		if reason == "" {
			reason = "pool-not-healthy"
		}
		c.logSV2StaleOrPausedWork(submit, job, minerHeader80, reason)
	}
	uncappedRequestedDiff := info.requestedDiff
	assignedDiff := info.assignedDiff
	assignedTargetLE := info.assignedTarget
	if uncappedRequestedDiff <= 0 {
		uncappedRequestedDiff = assignedDiff
	}
	if assignedDiff <= 0 {
		uncappedRequestedDiff = c.difficulty
		assignedDiff = c.capDifficultyForJob(c.difficulty, job)
		assignedTargetLE = c.targetForDifficulty(assignedDiff)
	}
	if sv2TargetIsZero(assignedTargetLE) {
		assignedTargetLE = c.targetForDifficulty(assignedDiff)
	}
	standardExtranonce := append([]byte(nil), info.standardExtranonce...)
	if len(standardExtranonce) == 0 {
		standardExtranonce = make([]byte, 0, len(c.extranonce1)+job.Extranonce2Size)
		standardExtranonce = append(standardExtranonce, c.extranonce1...)
		if job.Extranonce2Size > 0 {
			standardExtranonce = append(standardExtranonce, make([]byte, job.Extranonce2Size)...)
		}
	}

	variants := make([]sv2SubmitVariant, 0, 3)
	if c.isExtended {
		// Extended submit interoperability:
		// - Some firmware sends only the mutable tail.
		// - Some firmware sends the full extranonce blob used for hashing.
		// - Some firmware interprets extranonce_size as total (prefix+tail),
		//   then sends that full total-size blob in submit.
		tailLen := int(c.extranonceSize)
		fullLen := len(c.extranonce1) + tailLen
		switch len(extranonce) {
		case fullLen:
			if !bytes.Equal(extranonce[:len(c.extranonce1)], c.extranonce1) {
				logger.Warn("sv2 invalid extended extranonce prefix", "component", "stratum", "kind", "sv2", "remote", c.id,
					"sv2_trace", func() string {
						tCtx := c.newSV2SubmitTraceCtx(submit, info)
						tCtx.Result = "rejected"
						tCtx.Reason = "bad-extranonce-prefix"
						return BuildSV2Trace(tCtx)
					}())
				c.sendShareError(submit.ChannelID, submit.SequenceNumber, "bad-extranonce-prefix")
				return
			}
			variants = append(variants, sv2SubmitVariant{
				name:            "tail_only",
				en1:             c.extranonce1,
				en2:             extranonce[len(c.extranonce1):],
				extranonce2Size: tailLen,
				templateEx2Size: tailLen,
			})
			variants = append(variants, sv2SubmitVariant{
				name:            "prefix_plus_tail",
				en1:             c.extranonce1,
				en2:             extranonce[len(c.extranonce1):],
				extranonce2Size: tailLen,
				templateEx2Size: tailLen,
			})
		case tailLen:
			variants = append(variants, sv2SubmitVariant{
				name:            "tail_only",
				en1:             c.extranonce1,
				en2:             extranonce,
				extranonce2Size: tailLen,
				templateEx2Size: tailLen,
			})
			if false {
				if len(c.extranonce1) > 0 && len(extranonce) > len(c.extranonce1) && bytes.Equal(extranonce[:len(c.extranonce1)], c.extranonce1) {
					totalTail := len(extranonce) - len(c.extranonce1)
					variants = append(variants, sv2SubmitVariant{
						name:            "total_size_with_prefix",
						en1:             c.extranonce1,
						en2:             extranonce[len(c.extranonce1):],
						extranonce2Size: totalTail,
						templateEx2Size: totalTail,
					})
				}
				variants = append(variants, sv2SubmitVariant{
					name:            "full_submitted",
					en1:             nil,
					en2:             extranonce,
					extranonce2Size: len(extranonce),
					templateEx2Size: len(extranonce),
				})
				// Diagnostic: miner ignores ExtraNoncePrefix and uses all-zero en1.
				if len(c.extranonce1) > 0 {
					zeroEn1 := make([]byte, len(c.extranonce1))
					variants = append(variants, sv2SubmitVariant{
						name:            "zeroed_en1",
						en1:             zeroEn1,
						en2:             extranonce,
						extranonce2Size: tailLen,
						templateEx2Size: tailLen,
					})
					// Diagnostic: miner sends en1 in reversed byte order.
					revEn1 := append([]byte(nil), c.extranonce1...)
					for i, j := 0, len(revEn1)-1; i < j; i, j = i+1, j-1 {
						revEn1[i], revEn1[j] = revEn1[j], revEn1[i]
					}
					if !bytes.Equal(revEn1, c.extranonce1) {
						variants = append(variants, sv2SubmitVariant{
							name:            "en1_reversed",
							en1:             revEn1,
							en2:             extranonce,
							extranonce2Size: tailLen,
							templateEx2Size: tailLen,
						})
					}
				}
			}
		default:
			logger.Warn("sv2 invalid extended extranonce size", "component", "stratum", "kind", "sv2", "remote", c.id,
				"got", len(extranonce), "want_full", fullLen, "want_tail", tailLen,
				"sv2_trace", func() string {
					tCtx := c.newSV2SubmitTraceCtx(submit, info)
					tCtx.Result = "rejected"
					tCtx.Reason = "bad-extranonce-size"
					return BuildSV2Trace(tCtx)
				}())
			c.sendShareError(submit.ChannelID, submit.SequenceNumber, "bad-extranonce-size")
			return
		}
	} else {
		// Standard channels are header-only mining. Keep a single deterministic mode.
		variants = append(variants, sv2SubmitVariant{
			name:            "standard_direct",
			en1:             standardExtranonce,
			en2:             nil,
			extranonce2Size: 0,
			templateEx2Size: 0,
		})
	}
	// Some downstream firmware uses different byte ordering conventions when
	// serializing extranonce bytes in submits. Try a small compatibility set.
	if c.isExtended && false {
		expanded := make([]sv2SubmitVariant, 0, len(variants)*4)
		for _, v := range variants {
			expanded = append(expanded, v)

			if len(v.en2) > 1 {
				rev := append([]byte(nil), v.en2...)
				for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
					rev[i], rev[j] = rev[j], rev[i]
				}
				if !bytes.Equal(rev, v.en2) {
					nv := v
					nv.name = v.name + "_rev"
					nv.en2 = rev
					expanded = append(expanded, nv)
				}
			}

			if len(v.en2) >= 4 {
				dword := append([]byte(nil), v.en2...)
				dword[0], dword[1], dword[2], dword[3] = dword[3], dword[2], dword[1], dword[0]
				if !bytes.Equal(dword, v.en2) {
					nv := v
					nv.name = v.name + "_dword0"
					nv.en2 = dword
					expanded = append(expanded, nv)
				}
			}

			if len(v.en2) >= 2 {
				pair := append([]byte(nil), v.en2...)
				for i := 0; i+1 < len(pair); i += 2 {
					pair[i], pair[i+1] = pair[i+1], pair[i]
				}
				if !bytes.Equal(pair, v.en2) {
					nv := v
					nv.name = v.name + "_pairswap"
					nv.en2 = pair
					expanded = append(expanded, nv)
				}
			}
		}
		variants = expanded
	}

	ntimeHex := fmt.Sprintf("%08x", submit.NTime)
	nonceHex := fmt.Sprintf("%08x", submit.Nonce)
	ntimeHexes := []string{ntimeHex}
	nonceHexes := []string{nonceHex}
	// SV2 submit fields are already decoded from little-endian wire values into
	// host uint32s. Re-trying byte-swapped nTime/nonce variants can produce
	// headers that look low-diff but are invalid to bitcoind (for example,
	// submitblock "time-too-new").
	// Only accept canonical prevhash/bits from the active template. Permissive
	// byte-swap fallbacks can produce headers that don't extend the node's tip
	// and lead to submitblock rejections such as "prev-blk-not-found".
	prevHexes := []string{job.Template.Previous}
	bitsHexes := []string{job.Template.Bits}
	versionMask := job.VersionMask
	if versionMask == 0 {
		versionMask = c.cfg.VersionMask
	}
	versionResolution := resolveSubmittedVersion(uint32(job.Template.Version), submit.Version, versionMask, true)
	versionsToTry := []uint32{versionResolution.useVersion}
	if versionResolution.hasAlternateVersion {
		versionsToTry = append(versionsToTry, versionResolution.alternateUseVersion)
	}
	scriptTimesToTry := []int64{job.ScriptTime}
	submitScriptTime := int64(submit.NTime)
	if submitScriptTime > 0 && submitScriptTime != scriptTimesToTry[0] {
		scriptTimesToTry = append(scriptTimesToTry, submitScriptTime)
	}

	shareTargetBE := uint256BEFromBigInt(sv2TargetLEToBigInt(assignedTargetLE))
	hasNetworkTarget := job.Target != nil && job.Target.Sign() > 0
	var networkTargetBE [32]byte
	if hasNetworkTarget {
		networkTargetBE = uint256BEFromBigInt(job.Target)
	}
	shareThreshold := assignedDiff
	if shareThreshold <= 0 {
		shareThreshold = defaultMinDifficulty
	}
	var (
		blockHex      string
		headerHashLE  [32]byte
		headerHashRaw [32]byte
		hashHex       string
		selectedVer   uint32
		selectedMode  string
		selectedDiff  float64
		selectedSTime int64
		selectedNTime string
		selectedNonce string
		selectedPrev  string
		selectedBits  string
		selectedMerkleRootWireLE string
		selectedMerkleRootDisplayBE string
		selectedHeaderHex string
		selectedCoinbasePrefixHex string
		selectedCoinbaseSuffixHex string
		selectedFullCoinbaseHex string
		selectedCoinbaseTxIDDisplayBE string
		selectedCoinbaseTxIDLE string
		selectedMerkleLayersWireLE []string
		selectedMerkleLayersBE []string
		selectedBranches []string
		selectedEn2   []byte
		selectedModeExperimental bool
		selectedSubmitCoinbaseTxIDWireLE string
		selectedSubmitMerkleRootWireLE string
		selectedHeaderMerkleRootWireLE string
		selectedSubmitTxIDsWireLE []string
		selectedSubmitTxCount int
		selectedSubmitMerkleMatchesHeader bool
		acceptedShare bool
		maxTriedDiff  float64
		maxTriedMode  string
		maxTriedHash  string
	)
	candidateHeaderModes := make(map[string][]string)
	// Decode the coinbase halves that were pre-built in sendJob and sent to the
	// miner. Using these directly (rather than re-serialising from PayoutScript)
	// ensures we hash the exact same multi-output coinbase the miner hashed.
	coinb1Bytes, coinb1Err := hex.DecodeString(info.coinb1)
	coinb2Bytes, coinb2Err := hex.DecodeString(info.coinb2)
	if coinb1Err != nil || coinb2Err != nil {
		logger.Warn("sv2 decode coinbase parts", "component", "stratum", "kind", "sv2",
			"remote", c.id, "coinb1_err", coinb1Err, "coinb2_err", coinb2Err)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "internal-error")
		return
	}
	type sv2CoinbaseAttempt struct {
		scriptTime int64
		coinb1     []byte
		coinb2     []byte
		fixedCoinbase []byte
	}
	coinbaseAttempts := make([]sv2CoinbaseAttempt, 0, 2)
	if c.isExtended {
		coinbaseAttempts = append(coinbaseAttempts, sv2CoinbaseAttempt{scriptTime: job.ScriptTime, coinb1: coinb1Bytes, coinb2: coinb2Bytes})
	} else {
		fixedStandardCoinbase := append([]byte(nil), info.standardCoinbaseTx...)
		if len(fixedStandardCoinbase) == 0 {
			fixedStandardCoinbase = make([]byte, 0, len(coinb1Bytes)+len(standardExtranonce)+len(coinb2Bytes))
			fixedStandardCoinbase = append(fixedStandardCoinbase, coinb1Bytes...)
			fixedStandardCoinbase = append(fixedStandardCoinbase, standardExtranonce...)
			fixedStandardCoinbase = append(fixedStandardCoinbase, coinb2Bytes...)
		}
		coinbaseAttempts = append(coinbaseAttempts, sv2CoinbaseAttempt{scriptTime: job.ScriptTime, coinb1: coinb1Bytes, coinb2: coinb2Bytes, fixedCoinbase: fixedStandardCoinbase})
	}
	if c.isExtended && submitScriptTime > 0 && submitScriptTime != job.ScriptTime {
		ex2Size := job.Extranonce2Size
		templateEx2Size := job.TemplateExtraNonce2Size
		if c.isExtended {
			downstreamEx2 := int(c.extranonceSize)
			if downstreamEx2 > 0 {
				ex2Size = downstreamEx2
				templateEx2Size = downstreamEx2
			}
		}
		altCoinb1, altCoinb2, altErr := c.buildPayoutCoinbasePartsWithScriptTime(job, ex2Size, templateEx2Size, submitScriptTime)
		if altErr != nil {
			logger.Warn("sv2 rebuild coinbase parts", "component", "stratum", "kind", "sv2",
				"remote", c.id, "error", altErr, "script_time", submitScriptTime)
		} else {
			altCoinb1Bytes, altCoinb1Err := hex.DecodeString(altCoinb1)
			altCoinb2Bytes, altCoinb2Err := hex.DecodeString(altCoinb2)
			if altCoinb1Err != nil || altCoinb2Err != nil {
				logger.Warn("sv2 decode rebuilt coinbase parts", "component", "stratum", "kind", "sv2",
					"remote", c.id, "coinb1_err", altCoinb1Err, "coinb2_err", altCoinb2Err,
					"script_time", submitScriptTime)
			} else {
				coinbaseAttempts = append(coinbaseAttempts, sv2CoinbaseAttempt{scriptTime: submitScriptTime, coinb1: altCoinb1Bytes, coinb2: altCoinb2Bytes})
			}
		}
	}

	for _, coinbaseAttempt := range coinbaseAttempts {
		for _, variant := range variants {
			cb := make([]byte, 0, len(coinbaseAttempt.coinb1)+len(variant.en1)+len(variant.en2)+len(coinbaseAttempt.coinb2))
			if !c.isExtended && len(coinbaseAttempt.fixedCoinbase) > 0 {
				cb = append(cb, coinbaseAttempt.fixedCoinbase...)
			} else {
				cb = append(cb, coinbaseAttempt.coinb1...)
				cb = append(cb, variant.en1...)
				cb = append(cb, variant.en2...)
				cb = append(cb, coinbaseAttempt.coinb2...)
			}
			coinbaseTxID, txidErr := txidFromSerializedTx(cb)
			if txidErr != nil {
				logger.Warn("sv2 parse coinbase txid", "component", "stratum", "kind", "sv2",
					"remote", c.id, "error", txidErr, "variant", variant.name)
				continue
			}
			merkleTrace, merkleOK := buildSV2MerkleTrace(coinbaseTxID[:], job.MerkleBranches)
			if !merkleOK {
				continue
			}
			merkleRootWireLE, merkleErr := hex.DecodeString(merkleTrace.MerkleRootWireLE)
			if merkleErr != nil || len(merkleRootWireLE) != 32 {
				continue
			}
			for _, candidateVersion := range versionsToTry {
				if acceptedShare {
					break
				}
				for _, candidateNTimeHex := range ntimeHexes {
					if acceptedShare {
						break
					}
					for _, candidateNonceHex := range nonceHexes {
						if acceptedShare {
							break
						}
						for _, candidatePrevHex := range prevHexes {
							if acceptedShare {
								break
							}
							for _, candidateBitsHex := range bitsHexes {
								hdr, headerErr := buildBlockHeaderFromHex(int32(candidateVersion), candidatePrevHex, merkleRootWireLE, candidateNTimeHex, candidateBitsHex, candidateNonceHex)
								if headerErr != nil {
									logger.Warn("sv2 build block header", "component", "stratum", "kind", "sv2",
										"remote", c.id, "error", headerErr, "variant", variant.name,
										"ntime_hex", candidateNTimeHex, "nonce_hex", candidateNonceHex,
										"prev_hex", candidatePrevHex, "bits_hex", candidateBitsHex)
									continue
								}
								candidateHashRaw := doubleSHA256(hdr)
								var candidateHashLE [32]byte
								copy(candidateHashLE[:], candidateHashRaw)
								reverseBytes32(&candidateHashLE)
								candidateDiff := difficultyFromHash(candidateHashRaw)
								if candidateDiff > maxTriedDiff {
									maxTriedDiff = candidateDiff
									maxTriedMode = variant.name
									maxTriedHash = hex.EncodeToString(candidateHashLE[:])
								}
								candidateIsBlock := hasNetworkTarget && uint256BELessOrEqual(candidateHashLE, networkTargetBE)
								meetsShareTarget := uint256BELessOrEqual(candidateHashLE, shareTargetBE)
								networkTargetCompareHex := ""
								if hasNetworkTarget {
									networkTargetCompareHex = hex.EncodeToString(networkTargetBE[:])
								}
								logger.Debug("sv2 candidate evaluation", "component", "stratum", "kind", "sv2_candidate_eval",
									"remote", c.id, "worker", c.workerName,
									"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
									"sv2_trace", func() string {
										tCtx := c.newSV2SubmitTraceCtx(submit, info)
										tCtx.Ver = fmt.Sprintf("%08x", candidateVersion)
										tCtx.NTime = candidateNTimeHex
										tCtx.Nonce = candidateNonceHex
										tCtx.NBits = candidateBitsHex
										tCtx.Prev = candidatePrevHex
										tCtx.ExN = hex.EncodeToString(append(append([]byte(nil), variant.en1...), variant.en2...))
										tCtx.ExNLen = fmt.Sprintf("%d", len(variant.en1)+len(variant.en2))
										tCtx.CoinbaseTxID = merkleTrace.CoinbaseTxIDDisplayBE
										tCtx.Merkle = merkleTrace.MerkleRootDisplayBE
										tCtx.Hdr80 = hex.EncodeToString(hdr)
										tCtx.Hash = hex.EncodeToString(candidateHashLE[:])
										tCtx.SDiff = sv2FormatFloat(candidateDiff)
										tCtx.TDiff = sv2FormatFloat(assignedDiff)
										tCtx.NDiff = "-"
										tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
										tCtx.NetTarget = networkTargetCompareHex
										tCtx.Result = "-"
										tCtx.Reason = "-"
										tCtx.Mode = variant.name
										tCtx.JobType = info.jobType
										return BuildSV2Trace(tCtx)
									}(),
									"seq_job_id", submit.JobID, "template_job_id", job.JobID,
									"extranonce_hex", hex.EncodeToString(extranonce), "extranonce_len", len(extranonce),
									"extranonce_prefix_hex", hex.EncodeToString(variant.en1),
									"extranonce_tail_hex", hex.EncodeToString(variant.en2),
									"canonical_extranonce_full_hex", hex.EncodeToString(append(append([]byte(nil), variant.en1...), variant.en2...)),
									"mode", variant.name,
									"version", fmt.Sprintf("%08x", candidateVersion),
									"ntime", candidateNTimeHex,
									"nonce", candidateNonceHex,
									"nbits", candidateBitsHex,
									"prevhash_be", candidatePrevHex,
									"coinbase_prefix_hex", hex.EncodeToString(coinbaseAttempt.coinb1),
									"coinbase_suffix_hex", hex.EncodeToString(coinbaseAttempt.coinb2),
									"full_coinbase_hex", hex.EncodeToString(cb),
									"coinbase_txid_display_be", merkleTrace.CoinbaseTxIDDisplayBE,
									"coinbase_txid_wire_le", merkleTrace.CoinbaseTxIDWireLE,
									"merkle_branches_hex", merkleTrace.MerkleBranchesHex,
									"merkle_layer_hashes_wire_le", merkleTrace.MerkleLayerHashesWireLE,
									"merkle_layer_hashes_be", merkleTrace.MerkleLayerHashesBE,
									"merkle_root_wire_le", merkleTrace.MerkleRootWireLE,
									"merkle_root_display_be", merkleTrace.MerkleRootDisplayBE,
									"full_header80_hex", hex.EncodeToString(hdr),
									"hash", hex.EncodeToString(candidateHashLE[:]),
									"double_sha256_header_be", hex.EncodeToString(candidateHashLE[:]),
									"double_sha256_header_le", hex.EncodeToString(candidateHashRaw),
									"double_sha256_header_display_be", hex.EncodeToString(candidateHashLE[:]),
									"double_sha256_header_wire_le", hex.EncodeToString(candidateHashRaw),
									"header_hash_display_be", hex.EncodeToString(candidateHashLE[:]),
									"header_hash_wire_le", hex.EncodeToString(candidateHashRaw),
									"header_hash_compare_be", hex.EncodeToString(candidateHashLE[:]),
									"comparison_basis", "be",
									"share_compare_hash_uint256_be", hex.EncodeToString(candidateHashLE[:]),
									"share_compare_target_uint256_be", hex.EncodeToString(shareTargetBE[:]),
									"network_compare_hash_uint256_be", hex.EncodeToString(candidateHashLE[:]),
									"network_compare_target_uint256_be", networkTargetCompareHex,
									"share_target_result", meetsShareTarget,
									"network_target_result", candidateIsBlock,
								)
								hdrHex := hex.EncodeToString(hdr)
								candidateDescriptor := fmt.Sprintf("%s|v=%08x|nt=%s|nn=%s|bits=%s|prev=%s", variant.name, candidateVersion, candidateNTimeHex, candidateNonceHex, candidateBitsHex, candidatePrevHex)
								candidateHeaderModes[hdrHex] = append(candidateHeaderModes[hdrHex], candidateDescriptor)
								isCanonicalMode := sv2ModeIsCanonicalAuthoritative(variant.name, hdrHex, candidateHeaderModes)
								isExperimentalMode := !isCanonicalMode
										allowExperimentalMode := false && isExperimentalMode
								shareAuthoritative := isCanonicalMode
								blockAuthoritative := isCanonicalMode || allowExperimentalMode
								allowAuthoritativeMode := shareAuthoritative || blockAuthoritative
								if (candidateIsBlock || meetsShareTarget) && !allowAuthoritativeMode {
									logger.Warn("sv2 candidate mode excluded from authoritative selection", "component", "stratum", "kind", "sv2_candidate_mode_excluded",
										"remote", c.id, "worker", c.workerName,
										"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
										"mode", variant.name,
										"share_target_result", meetsShareTarget,
										"network_target_result", candidateIsBlock,
												"experimental_enabled", false,
										"reason", "canonical-only authoritative selection")
								}
								if candidateIsBlock && isExperimentalMode {
									logger.Warn("sv2 non-canonical network-target diagnostic", "component", "stratum", "kind", "sv2_noncanonical_network_target",
										"remote", c.id, "worker", c.workerName,
										"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
										"mode", variant.name,
												"experimental_enabled", false,
										"accepted_without_submitblock", false)
								}
								if candidateIsBlock && !meetsShareTarget {
									logger.Warn("sv2 candidate network-target-only", "component", "stratum", "kind", "sv2_candidate_network_only",
										"remote", c.id, "worker", c.workerName,
										"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
										"mode", variant.name,
										"full_header80_hex", hex.EncodeToString(hdr),
										"double_sha256_header_display_be", hex.EncodeToString(candidateHashLE[:]),
										"double_sha256_header_wire_le", hex.EncodeToString(candidateHashRaw),
										"reason", "not selected by network target alone")
								}
								if (meetsShareTarget && shareAuthoritative) || (candidateIsBlock && blockAuthoritative) {
									var buf bytes.Buffer
									buf.Write(hdr)
									writeVarInt(&buf, uint64(1+len(job.Transactions)))
									buf.Write(cb)
									submitTxIDs := make([][32]byte, 0, 1+len(job.Transactions))
									coinbaseTxIDArr, txidErr := txidFromSerializedTx(cb)
									if txidErr != nil {
										logger.Warn("sv2 parse submit coinbase txid", "component", "stratum", "kind", "sv2",
											"remote", c.id, "error", txidErr)
										continue
									}
									submitTxIDs = append(submitTxIDs, coinbaseTxIDArr)
									submitTxIDsWire := make([]string, 0, 1+len(job.Transactions))
									submitTxIDsWire = append(submitTxIDsWire, hex.EncodeToString(coinbaseTxIDArr[:]))
									txBuildOk := true
									for _, tx := range job.Transactions {
										txRaw, txErr := hex.DecodeString(tx.Data)
										if txErr != nil {
											logger.Warn("sv2 decode tx data", "component", "stratum", "kind", "sv2",
												"remote", c.id, "error", txErr)
											txBuildOk = false
											break
										}
										buf.Write(txRaw)
										txid, txidErr := txidFromSerializedTx(txRaw)
										if txidErr != nil {
											logger.Warn("sv2 parse txid from tx data", "component", "stratum", "kind", "sv2",
												"remote", c.id, "error", txidErr)
											txBuildOk = false
											break
										}
										submitTxIDs = append(submitTxIDs, txid)
										submitTxIDsWire = append(submitTxIDsWire, hex.EncodeToString(txid[:]))
									}
									if !txBuildOk {
										continue
									}
									recomputedMerkle, merkleOK := sv2ComputeMerkleRootFromTxIDs(submitTxIDs)
									if !merkleOK {
										continue
									}
									blockHex = hex.EncodeToString(buf.Bytes())
									headerHashLE = candidateHashLE
									headerHashRaw = [32]byte{}
									copy(headerHashRaw[:], candidateHashRaw)
									hashHex = hex.EncodeToString(headerHashLE[:])
									selectedVer = candidateVersion
									selectedMode = variant.name
									selectedDiff = candidateDiff
									selectedSTime = coinbaseAttempt.scriptTime
									selectedNTime = candidateNTimeHex
									selectedNonce = candidateNonceHex
									selectedPrev = candidatePrevHex
									selectedBits = candidateBitsHex
									selectedMerkleRootWireLE = merkleTrace.MerkleRootWireLE
									selectedMerkleRootDisplayBE = merkleTrace.MerkleRootDisplayBE
									selectedHeaderHex = hex.EncodeToString(hdr)
									selectedCoinbasePrefixHex = hex.EncodeToString(coinbaseAttempt.coinb1)
									selectedCoinbaseSuffixHex = hex.EncodeToString(coinbaseAttempt.coinb2)
									selectedFullCoinbaseHex = hex.EncodeToString(cb)
									selectedCoinbaseTxIDDisplayBE = merkleTrace.CoinbaseTxIDDisplayBE
									selectedCoinbaseTxIDLE = merkleTrace.CoinbaseTxIDWireLE
									selectedMerkleLayersWireLE = append([]string(nil), merkleTrace.MerkleLayerHashesWireLE...)
									selectedMerkleLayersBE = append([]string(nil), merkleTrace.MerkleLayerHashesBE...)
									selectedBranches = append([]string(nil), merkleTrace.MerkleBranchesHex...)
									selectedEn2 = append([]byte(nil), variant.en2...)
									selectedModeExperimental = isExperimentalMode
									selectedSubmitCoinbaseTxIDWireLE = hex.EncodeToString(coinbaseTxIDArr[:])
									selectedSubmitMerkleRootWireLE = hex.EncodeToString(recomputedMerkle[:])
									selectedHeaderMerkleRootWireLE = hex.EncodeToString(hdr[36:68])
									selectedSubmitTxIDsWireLE = append([]string(nil), submitTxIDsWire...)
									selectedSubmitTxCount = len(submitTxIDsWire)
									selectedSubmitMerkleMatchesHeader = bytes.Equal(recomputedMerkle[:], hdr[36:68])
									acceptedShare = true
									break
								}
								if hashHex == "" {
									headerHashLE = candidateHashLE
									headerHashRaw = [32]byte{}
									copy(headerHashRaw[:], candidateHashRaw)
									hashHex = hex.EncodeToString(headerHashLE[:])
									selectedVer = candidateVersion
									selectedMode = variant.name
									selectedDiff = candidateDiff
									selectedSTime = coinbaseAttempt.scriptTime
									selectedNTime = candidateNTimeHex
									selectedNonce = candidateNonceHex
									selectedPrev = candidatePrevHex
									selectedBits = candidateBitsHex
									selectedMerkleRootWireLE = merkleTrace.MerkleRootWireLE
									selectedMerkleRootDisplayBE = merkleTrace.MerkleRootDisplayBE
									selectedHeaderHex = hex.EncodeToString(hdr)
									selectedCoinbasePrefixHex = hex.EncodeToString(coinbaseAttempt.coinb1)
									selectedCoinbaseSuffixHex = hex.EncodeToString(coinbaseAttempt.coinb2)
									selectedFullCoinbaseHex = hex.EncodeToString(cb)
									selectedCoinbaseTxIDDisplayBE = merkleTrace.CoinbaseTxIDDisplayBE
									selectedCoinbaseTxIDLE = merkleTrace.CoinbaseTxIDWireLE
									selectedMerkleLayersWireLE = append([]string(nil), merkleTrace.MerkleLayerHashesWireLE...)
									selectedMerkleLayersBE = append([]string(nil), merkleTrace.MerkleLayerHashesBE...)
									selectedBranches = append([]string(nil), merkleTrace.MerkleBranchesHex...)
									selectedEn2 = append([]byte(nil), variant.en2...)
									selectedModeExperimental = isExperimentalMode
								}
							}
							if acceptedShare {
								break
							}
						}
					}
				}
			}
			if acceptedShare {
				break
			}
		}
		if acceptedShare {
			break
		}
	}
	if len(candidateHeaderModes) > 1 {
		headers := make([]string, 0, len(candidateHeaderModes))
		modesByHeader := make(map[string][]string, len(candidateHeaderModes))
		for hdrHex, modes := range candidateHeaderModes {
			headers = append(headers, hdrHex)
			modesByHeader[hdrHex] = append([]string(nil), modes...)
		}
		headerDiffs := make(map[string][]int)
		for _, hdrHex := range headers {
			if selectedHeaderHex != "" && hdrHex != selectedHeaderHex {
				headerDiffs[hdrHex] = diffHeaderHexByteOffsets(selectedHeaderHex, hdrHex, 24)
			}
		}
		logger.Warn("sv2 reconstruction mode header mismatch", "component", "stratum", "kind", "sv2_reconstruction_header_mismatch",
			"remote", c.id, "worker", c.workerName,
			"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
			"unique_header_count", len(candidateHeaderModes),
			"selected_mode", selectedMode,
			"selected_header80_hex", selectedHeaderHex,
			"modes_by_header", modesByHeader,
			"header_diff_offsets_vs_selected", headerDiffs)
	}
	computedShareDiff := 0.0
	meetsShareTarget := false
	if headerHashRaw != ([32]byte{}) {
		vm := computeShareValidationMath(headerHashLE, shareThreshold)
		shareTargetBE = vm.ShareTargetBE
		meetsShareTarget = vm.MeetsShareTarget
		computedShareDiff = vm.ComputedShareDiffBDiff
	}
	meetsNetworkTarget := hasNetworkTarget && headerHashLE != ([32]byte{}) && uint256BELessOrEqual(headerHashLE, networkTargetBE)
	networkDiffBDiff := 0.0
	if bitsHexForDiff := selectedBits; bitsHexForDiff != "" {
		if nbitsU32, err := parseUint32BEHexPadded(bitsHexForDiff); err == nil {
			networkDiffBDiff = difficultyFromBits(nbitsU32)
		}
	} else if nbitsU32, err := parseUint32BEHexPadded(job.Template.Bits); err == nil {
		networkDiffBDiff = difficultyFromBits(nbitsU32)
	}
	selectedVersionHex := fmt.Sprintf("%08x", selectedVer)
	if selectedVer == 0 {
		selectedVersionHex = fmt.Sprintf("%08x", submit.Version)
	}
	selectedNTimeHex := selectedNTime
	if selectedNTimeHex == "" {
		selectedNTimeHex = fmt.Sprintf("%08x", submit.NTime)
	}
	selectedNonceHex := selectedNonce
	if selectedNonceHex == "" {
		selectedNonceHex = fmt.Sprintf("%08x", submit.Nonce)
	}
	selectedPrevHex := selectedPrev
	if selectedPrevHex == "" {
		selectedPrevHex = job.Template.Previous
	}
	selectedBitsHex := selectedBits
	if selectedBitsHex == "" {
		selectedBitsHex = job.Template.Bits
	}
	networkTargetHex := ""
	if hasNetworkTarget {
		networkTargetHex = hex.EncodeToString(networkTargetBE[:])
	}
	activeTargetBeforeSubmit := c.activeTarget
	activeTargetAtSubmit := c.activeTarget
	logger.Debug("sv2 reconstruction trace", "component", "stratum", "kind", "sv2_reconstruction_trace",
		"sv2_trace", func() string {
			tCtx := c.newSV2SubmitTraceCtx(submit, info)
			tCtx.Ver = selectedVersionHex
			tCtx.NTime = selectedNTimeHex
			tCtx.Nonce = selectedNonceHex
			tCtx.NBits = selectedBitsHex
			tCtx.Prev = selectedPrevHex
			tCtx.ExN = hex.EncodeToString(standardExtranonce)
			tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
			tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
			tCtx.Merkle = selectedMerkleRootDisplayBE
			tCtx.Hdr80 = selectedHeaderHex
			tCtx.Hash = hashHex
			tCtx.SDiff = sv2FormatFloat(computedShareDiff)
			tCtx.TDiff = sv2FormatFloat(shareThreshold)
			tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
			tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
			tCtx.NetTarget = networkTargetHex
			tCtx.Result = "-"
			tCtx.Reason = "-"
			tCtx.Mode = selectedMode
			tCtx.JobType = info.jobType
			return BuildSV2Trace(tCtx)
		}(),
		"seq_job_id", submit.JobID,
		"template_job_id", job.JobID,
		"channel_id", submit.ChannelID,
		"channel_type", info.channelType,
		"job_type", info.jobType,
		"validation_mode", selectedMode,
		"seq", submit.SequenceNumber,
		"extranonce_hex", hex.EncodeToString(extranonce),
		"extranonce_len", len(extranonce),
		"version", selectedVersionHex,
		"ntime", selectedNTimeHex,
		"nonce", selectedNonceHex,
		"prevhash_be", selectedPrevHex,
		"nbits", selectedBitsHex,
		"selected_mode", selectedMode,
		"coinbase_prefix_hex", selectedCoinbasePrefixHex,
		"coinbase_suffix_hex", selectedCoinbaseSuffixHex,
		"full_coinbase_hex", selectedFullCoinbaseHex,
		"coinbase_txid_display_be", selectedCoinbaseTxIDDisplayBE,
		"coinbase_txid_wire_le", selectedCoinbaseTxIDLE,
		"merkle_branches_hex", selectedBranches,
		"merkle_layer_hashes_wire_le", selectedMerkleLayersWireLE,
		"merkle_layer_hashes_be", selectedMerkleLayersBE,
		"merkle_root_wire_le", selectedMerkleRootWireLE,
		"merkle_root_display_be", selectedMerkleRootDisplayBE,
		"full_header80_hex", selectedHeaderHex,
		"double_sha256_header_be", hashHex,
		"double_sha256_header_le", hex.EncodeToString(headerHashRaw[:]),
		"double_sha256_header_display_be", hashHex,
		"double_sha256_header_wire_le", hex.EncodeToString(headerHashRaw[:]),
		"selected_mode_experimental", selectedModeExperimental,
	)
	if len(minerHeader80) == 80 {
		espHeaderHex := hex.EncodeToString(minerHeader80)
		diffOffsets := diffHeaderHexByteOffsets(selectedHeaderHex, espHeaderHex, 80)
		merkleOnlyMismatch := sv2DiffIsMerkleRootOnly(diffOffsets)
		espHashRaw := doubleSHA256(minerHeader80)
		var espHashBE [32]byte
		copy(espHashBE[:], espHashRaw)
		reverseBytes32(&espHashBE)
		espHashHex := hex.EncodeToString(espHashBE[:])
		reconstructedHashHex := hex.EncodeToString(headerHashLE[:])
		espHashMatches := espHashHex == reconstructedHashHex
		espMeetsShare := uint256BELessOrEqual(espHashBE, shareTargetBE)
		reconstructedMeetsShare := uint256BELessOrEqual(headerHashLE, shareTargetBE)
		logger.Debug("sv2 header parity", "component", "stratum", "kind", "sv2_header_parity",
			"remote", c.id, "worker", c.workerName,
			"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
			"selected_mode", selectedMode,
			"esp_header80_hex", espHeaderHex,
			"full_header80_hex", selectedHeaderHex,
			"header_match", selectedHeaderHex == espHeaderHex,
			"header_diff_offsets", diffOffsets,
			"header_diff_count", len(diffOffsets),
			"merkle_root_only_mismatch", merkleOnlyMismatch,
			"merkle_root_field_offsets", "36..67",
			"diagnostic_hint", "if only 36..67 differ, issue is coinbase/merkle root reconstruction")
		if info.channelType == sv2ChannelTypeStandard {
			logger.Debug("sv2 standard parity", "component", "stratum", "kind", "sv2_standard_parity",
				"remote", c.id, "worker", c.workerName,
				"channel_type", info.channelType,
				"job_id", submit.JobID,
				"seq_job_id", submit.JobID,
				"template_job_id", job.JobID,
				"seq", submit.SequenceNumber,
				"version", fmt.Sprintf("%08x", submit.Version),
				"ntime", fmt.Sprintf("%08x", submit.NTime),
				"nonce", fmt.Sprintf("%08x", submit.Nonce),
				"esp_header80_hex", espHeaderHex,
				"reconstructed_header80_hex", selectedHeaderHex,
				"byte_diff_offsets", diffOffsets,
				"merkle_only_36_67", merkleOnlyMismatch,
				"esp_hash_be", espHashHex,
				"reconstructed_hash_be", reconstructedHashHex,
				"esp_hash_matches_reconstructed", espHashMatches,
				"esp_meets_share_target", espMeetsShare,
				"reconstructed_meets_share_target", reconstructedMeetsShare)
			if !espHashMatches {
				reason := sv2ClassifyHeaderMismatch(diffOffsets)
				logger.Warn("sv2 standard header mismatch", "component", "stratum", "kind", "sv2_standard_header_mismatch",
					"remote", c.id, "worker", c.workerName,
					"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
					"sv2_trace", func() string {
						tCtx := c.newSV2SubmitTraceCtx(submit, info)
						tCtx.Ver = selectedVersionHex
						tCtx.NTime = selectedNTimeHex
						tCtx.Nonce = selectedNonceHex
						tCtx.NBits = selectedBitsHex
						tCtx.Prev = selectedPrevHex
						tCtx.ExN = hex.EncodeToString(standardExtranonce)
						tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
						tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
						tCtx.Merkle = selectedMerkleRootDisplayBE
						tCtx.Hdr80 = selectedHeaderHex
						tCtx.Hash = hashHex
						tCtx.Result = "rejected"
						tCtx.Reason = reason
						tCtx.Mode = selectedMode
						tCtx.JobType = info.jobType
						return BuildSV2Trace(tCtx)
					}(),
					"reason", reason,
					"esp_header80_hex", espHeaderHex,
					"reconstructed_header80_hex", selectedHeaderHex,
					"byte_diff_offsets", diffOffsets,
					"merkle_only_36_67", merkleOnlyMismatch)
				c.recordShare(c.workerName, false, 0, selectedDiff, reason, hashHex, now)
				c.sendShareError(submit.ChannelID, submit.SequenceNumber, reason)
				return
			}
		}
	}
	logger.Debug("share validation",
		"component", "stratum",
		"kind", "sv2",
		"sv2_trace", func() string {
			tCtx := c.newSV2SubmitTraceCtx(submit, info)
			tCtx.Ver = selectedVersionHex
			tCtx.NTime = selectedNTimeHex
			tCtx.Nonce = selectedNonceHex
			tCtx.NBits = selectedBitsHex
			tCtx.Prev = selectedPrevHex
			tCtx.ExN = hex.EncodeToString(standardExtranonce)
			tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
			tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
			tCtx.Merkle = selectedMerkleRootDisplayBE
			tCtx.Hdr80 = selectedHeaderHex
			tCtx.Hash = hashHex
			tCtx.SDiff = sv2FormatFloat(computedShareDiff)
			tCtx.TDiff = sv2FormatFloat(shareThreshold)
			tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
			tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
			tCtx.NetTarget = networkTargetHex
			tCtx.Result = "-"
			tCtx.Reason = "-"
			tCtx.Mode = selectedMode
			tCtx.JobType = info.jobType
			return BuildSV2Trace(tCtx)
		}(),
		"channel_type", info.channelType,
		"job_type", info.jobType,
		"validation_mode", selectedMode,
		"connection_worker", c.workerName,
		"submit_worker", c.workerName,
		"job_id", submit.JobID,
		"header_hash_display_be", hashHex,
		"header_hash_wire_le", hex.EncodeToString(headerHashRaw[:]),
		"header_hash_compare_be", hex.EncodeToString(headerHashLE[:]),
		"full_header80_hex", selectedHeaderHex,
		"double_sha256_header_be", hashHex,
		"double_sha256_header_le", hex.EncodeToString(headerHashRaw[:]),
		"double_sha256_header_display_be", hashHex,
		"double_sha256_header_wire_le", hex.EncodeToString(headerHashRaw[:]),
		"header_hex", selectedHeaderHex,
		"selected_mode_experimental", selectedModeExperimental,
		"coinbase_txid_display_be", selectedCoinbaseTxIDDisplayBE,
		"coinbase_txid_wire_le", selectedCoinbaseTxIDLE,
		"merkle_root_wire_le", selectedMerkleRootWireLE,
		"merkle_root_display_be", selectedMerkleRootDisplayBE,
		"share_target_be", hex.EncodeToString(shareTargetBE[:]),
		"network_target_be", networkTargetHex,
		"comparison_basis", "be",
		"share_compare_hash_uint256_be", hex.EncodeToString(headerHashLE[:]),
		"share_compare_target_uint256_be", hex.EncodeToString(shareTargetBE[:]),
		"network_compare_hash_uint256_be", hex.EncodeToString(headerHashLE[:]),
		"network_compare_target_uint256_be", networkTargetHex,
		"assigned_share_diff_bdiff", shareThreshold,
		"uncapped_requested_share_diff_bdiff", uncappedRequestedDiff,
		"computed_share_diff_bdiff", computedShareDiff,
		"network_diff_bdiff", networkDiffBDiff,
		"meets_share_target", meetsShareTarget,
		"meets_network_target", meetsNetworkTarget,
		"share_compare", "header_hash <= share_target",
		"network_compare", "header_hash <= network_target",
		"is_block", meetsNetworkTarget,
		"version", selectedVersionHex,
		"ntime", selectedNTimeHex,
		"nonce", selectedNonceHex,
		"nbits", selectedBitsHex,
		"prevhash_be", selectedPrevHex,
		"merkle_root_wire_le", selectedMerkleRootWireLE,
		"merkle_root_display_be", selectedMerkleRootDisplayBE,
		"channel_id", submit.ChannelID,
		"seq", submit.SequenceNumber,
		"extranonce_hex", hex.EncodeToString(extranonce),
		"extranonce_len", len(extranonce),
		"canonical_inserted_extranonce_hex", hex.EncodeToString(standardExtranonce),
		"canonical_inserted_extranonce_len", len(standardExtranonce),
		"active_channel_target_before_submit", hex.EncodeToString(activeTargetBeforeSubmit[:]),
		"active_channel_target_at_submit", hex.EncodeToString(activeTargetAtSubmit[:]),
		"template_job_id", job.JobID,
		"seq_job_id", submit.JobID,
		"selected_mode", selectedMode,
		"selected_mode_experimental", selectedModeExperimental,
	)
	if (debugLogging || verboseRuntimeLogging) && computedShareDiff > 0 {
		delta := math.Abs(computedShareDiff - selectedDiff)
		if delta/computedShareDiff > 1e-3 {
			logger.Warn("difficulty consistency mismatch", "component", "stratum", "kind", "sv2", "job_id", submit.JobID,
				"computed_share_diff_bdiff", computedShareDiff,
				"legacy_share_diff_value", selectedDiff,
				"delta", delta)
		}
	}
	if shareValidationDebugEnabled() {
		validationResult := "accepted"
		rejectReason := ""
		if !acceptedShare {
			validationResult = "rejected"
			rejectReason = "low-difficulty"
		}
		logger.Debug("share validation debug",
			"component", "stratum",
			"kind", "sv2",
			"stratum_version", "sv2",
			"connection_id", c.id,
			"remote", c.id,
			"worker_name", c.workerName,
			"channel_id", submit.ChannelID,
			"job_id", submit.JobID,
			"sequence_number", submit.SequenceNumber,
			"nonce", selectedNonceHex,
			"ntime", selectedNTimeHex,
			"version", selectedVersionHex,
			"extranonce", hex.EncodeToString(standardExtranonce),
			"extranonce2", hex.EncodeToString(selectedEn2),
			"prevhash", selectedPrevHex,
			"merkle_root", selectedMerkleRootDisplayBE,
			"nbits", selectedBitsHex,
			"header80_hex", selectedHeaderHex,
			"hash_hex", hashHex,
			"hash_interpreted_integer_hex", hex.EncodeToString(headerHashLE[:]),
			"assigned_target_hex", hex.EncodeToString(shareTargetBE[:]),
			"required_difficulty", shareThreshold,
			"computed_share_difficulty", computedShareDiff,
			"validation_result", validationResult,
			"reject_reason", rejectReason,
			"open_nominal_hash_rate", c.openNominalHashrate,
			"open_max_target", func() string {
				if c.hasMinerMaxTarget {
					return hex.EncodeToString(c.minerMaxTarget[:])
				}
				return ""
			}(),
			"open_success_target", hex.EncodeToString(c.activeTarget[:]),
			"active_channel_target_before_submit", hex.EncodeToString(activeTargetBeforeSubmit[:]),
			"active_channel_target_at_submit", hex.EncodeToString(activeTargetAtSubmit[:]),
		)
	}
	if !acceptedShare {
		variantNames := make([]string, 0, len(variants))
		for _, v := range variants {
			variantNames = append(variantNames, v.name)
		}
		altEndianDiff := 0.0
		if headerHashRaw != ([32]byte{}) {
			altEndianDiff = difficultyFromHash(headerHashLE[:])
		}
		lastDiffChangeAgoMs := int64(-1)
		if !c.lastDiffChangeSV2.IsZero() {
			lastDiffChangeAgoMs = now.Sub(c.lastDiffChangeSV2).Milliseconds()
		}
		logger.Warn("sv2 share rejected low-difficulty", "component", "stratum", "kind", "sv2_lowdiff_reject",
			"remote", c.id, "worker", c.workerName,
			"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
			"sv2_trace", func() string {
				tCtx := c.newSV2SubmitTraceCtx(submit, info)
				tCtx.Ver = selectedVersionHex
				tCtx.NTime = selectedNTimeHex
				tCtx.Nonce = selectedNonceHex
				tCtx.NBits = selectedBitsHex
				tCtx.Prev = selectedPrevHex
				tCtx.ExN = hex.EncodeToString(standardExtranonce)
				tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
				tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
				tCtx.Merkle = selectedMerkleRootDisplayBE
				tCtx.Hdr80 = selectedHeaderHex
				tCtx.Hash = hashHex
				tCtx.SDiff = sv2FormatFloat(computedShareDiff)
				tCtx.TDiff = sv2FormatFloat(shareThreshold)
				tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
				tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
				tCtx.NetTarget = networkTargetHex
				tCtx.Result = "rejected"
				tCtx.Reason = "low-difficulty"
				tCtx.Mode = selectedMode
				tCtx.JobType = info.jobType
				return BuildSV2Trace(tCtx)
			}(),
			"hash", hashHex,
			"computed_share_diff_bdiff", selectedDiff, "required_share_diff_bdiff", shareThreshold,
			"uncapped_requested_share_diff_bdiff", uncappedRequestedDiff,
			"max_tried_diff", maxTriedDiff, "max_tried_mode", maxTriedMode, "max_tried_hash", maxTriedHash,
			"alt_endian_diff", altEndianDiff,
			"assigned_share_diff_bdiff", assignedDiff, "current_share_diff_bdiff", c.difficulty, "prev_share_diff_bdiff", c.previousDifficulty,
			"last_diff_change_ago_ms", lastDiffChangeAgoMs,
			"submitted_ntime", submit.NTime, "submitted_nonce", submit.Nonce,
			"submitted_version", fmt.Sprintf("%08x", submit.Version), "selected_version", fmt.Sprintf("%08x", selectedVer),
			"selected_mode", selectedMode, "selected_ntime_hex", selectedNTime, "selected_nonce_hex", selectedNonce,
			"selected_prev_hex", selectedPrev, "selected_bits_hex", selectedBits,
			"submitted_extranonce_len", len(extranonce), "selected_extranonce2_len", len(selectedEn2),
			"modes_tried", variantNames)
		logger.Warn("sv2 low-diff reject diagnostics", "component", "stratum", "kind", "sv2_lowdiff_reject_diag",
			"remote", c.id, "worker", c.workerName, "hash", hashHex,
			"submitted_version", fmt.Sprintf("%08x", submit.Version), "selected_version", fmt.Sprintf("%08x", selectedVer),
			"selected_mode", selectedMode, "submitted_extranonce_len", len(extranonce), "modes_tried", variantNames,
			"submitted_extranonce_hex", hex.EncodeToString(extranonce), "extranonce1_hex", c.extranonce1Hex,
			"computed_share_diff_bdiff", selectedDiff, "required_share_diff_bdiff", shareThreshold,
			"uncapped_requested_share_diff_bdiff", uncappedRequestedDiff,
			"selected_script_time", selectedSTime, "script_times_tried", scriptTimesToTry,
			"selected_ntime_hex", selectedNTime, "selected_nonce_hex", selectedNonce,
			"ntime_hexes_tried", ntimeHexes, "nonce_hexes_tried", nonceHexes,
			"selected_prev_hex", selectedPrev, "selected_bits_hex", selectedBits,
			"prev_hexes_tried", prevHexes, "bits_hexes_tried", bitsHexes)
		logger.Warn("sv2 low-diff reject coinbase", "component", "stratum", "kind", "sv2_lowdiff_reject_coinbase",
			"remote", c.id, "worker", c.workerName, "hash", hashHex,
			"coinb1_hex", info.coinb1, "coinb2_len", len(info.coinb2)/2,
			"extranonce1_hex", c.extranonce1Hex, "extranonce1_len", len(c.extranonce1),
			"submitted_extranonce_hex", hex.EncodeToString(extranonce), "submitted_extranonce_len", len(extranonce),
			"modes_tried", variantNames)
		c.recordShare(c.workerName, false, 0, selectedDiff, "lowDiff", hashHex, now)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "low-difficulty")
		return
	}

	if c.isDuplicateShare(submit.JobID, selectedEn2, submit.NTime, submit.Nonce, selectedVer) {
		logger.Warn("sv2 duplicate share rejected", "component", "stratum", "kind", "sv2_duplicate_reject",
			"remote", c.id, "worker", c.workerName,
			"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
			"sv2_trace", func() string {
				tCtx := c.newSV2SubmitTraceCtx(submit, info)
				tCtx.Ver = selectedVersionHex
				tCtx.NTime = selectedNTimeHex
				tCtx.Nonce = selectedNonceHex
				tCtx.NBits = selectedBitsHex
				tCtx.Prev = selectedPrevHex
				tCtx.ExN = hex.EncodeToString(standardExtranonce)
				tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
				tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
				tCtx.Merkle = selectedMerkleRootDisplayBE
				tCtx.Hdr80 = selectedHeaderHex
				tCtx.Hash = hashHex
				tCtx.Result = "rejected"
				tCtx.Reason = "duplicate-share"
				tCtx.Mode = selectedMode
				tCtx.JobType = info.jobType
				return BuildSV2Trace(tCtx)
			}())
		c.recordShare(c.workerName, false, 0, selectedDiff, "duplicate share", hashHex, now)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "duplicate-share")
		return
	}

	isBlock := false
	if job.Target != nil && job.Target.Sign() > 0 {
		networkTargetBE := uint256BEFromBigInt(job.Target)
		isBlock = uint256BELessOrEqual(headerHashLE, networkTargetBE)
	}
	logger.Debug("sv2 share accepted", "component", "stratum", "kind", "sv2",
		"remote", c.id, "worker", c.workerName,
		"job_id", submit.JobID, "hash", hashHex, "is_block", isBlock, "mode", selectedMode,
		"sv2_trace", func() string {
			tCtx := c.newSV2SubmitTraceCtx(submit, info)
			tCtx.Ver = selectedVersionHex
			tCtx.NTime = selectedNTimeHex
			tCtx.Nonce = selectedNonceHex
			tCtx.NBits = selectedBitsHex
			tCtx.Prev = selectedPrevHex
			tCtx.ExN = hex.EncodeToString(standardExtranonce)
			tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
			tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
			tCtx.Merkle = selectedMerkleRootDisplayBE
			tCtx.Hdr80 = selectedHeaderHex
			tCtx.Hash = hashHex
			tCtx.SDiff = sv2FormatFloat(computedShareDiff)
			tCtx.TDiff = sv2FormatFloat(shareThreshold)
			tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
			tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
			tCtx.NetTarget = networkTargetHex
			if isBlock {
				tCtx.Result = "block-candidate"
			} else {
				tCtx.Result = "accepted"
			}
			tCtx.Reason = "-"
			tCtx.Mode = selectedMode
			tCtx.JobType = info.jobType
			return BuildSV2Trace(tCtx)
		}(),
		"computed_share_diff_bdiff", selectedDiff, "required_share_diff_bdiff", shareThreshold,
		"selected_script_time", selectedSTime,
		"selected_ntime_hex", selectedNTime, "selected_nonce_hex", selectedNonce,
		"selected_prev_hex", selectedPrev, "selected_bits_hex", selectedBits)

	blockFound := false
	authoritativeMode := ""
	if isBlock {
		logger.Debug("sv2 submitblock assembly", "component", "stratum", "kind", "sv2_submitblock_assembly",
			"remote", c.id, "worker", c.workerName,
			"channel_type", info.channelType,
			"job_type", info.jobType,
			"selected_mode", selectedMode,
			"template_job_id", job.JobID,
			"seq_job_id", submit.JobID,
			"job_id", submit.JobID,
			"submit_block_header80_hex", selectedHeaderHex,
			"submit_block_coinbase_hex", selectedFullCoinbaseHex,
			"submit_block_coinbase_txid_wire_le", selectedSubmitCoinbaseTxIDWireLE,
			"submit_block_merkle_root_wire_le", selectedSubmitMerkleRootWireLE,
			"header_merkle_root_wire_le", selectedHeaderMerkleRootWireLE,
			"submit_block_tx_count", selectedSubmitTxCount,
			"submit_block_txids_wire_le", selectedSubmitTxIDsWireLE,
			"submit_block_recomputed_merkle_matches_header", selectedSubmitMerkleMatchesHeader)
		if !selectedSubmitMerkleMatchesHeader {
			logger.Warn("sv2 submitblock assembly mismatch", "component", "stratum", "kind", "sv2_submitblock_assembly_mismatch",
				"remote", c.id, "worker", c.workerName,
				"sv2_trace", func() string {
					tCtx := c.newSV2SubmitTraceCtx(submit, info)
					tCtx.Ver = selectedVersionHex
					tCtx.NTime = selectedNTimeHex
					tCtx.Nonce = selectedNonceHex
					tCtx.NBits = selectedBitsHex
					tCtx.Prev = selectedPrevHex
					tCtx.ExN = hex.EncodeToString(standardExtranonce)
					tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
					tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
					tCtx.Merkle = selectedMerkleRootDisplayBE
					tCtx.Hdr80 = selectedHeaderHex
					tCtx.Hash = hashHex
					tCtx.Result = "block-rejected"
					tCtx.Reason = "submitblock-assembly-merkle-mismatch"
					tCtx.Mode = selectedMode
					tCtx.JobType = info.jobType
					return BuildSV2Trace(tCtx)
				}(),
				"channel_type", info.channelType,
				"job_type", info.jobType,
				"selected_mode", selectedMode,
				"template_job_id", job.JobID,
				"seq_job_id", submit.JobID,
				"submit_block_header80_hex", selectedHeaderHex,
				"submit_block_coinbase_hex", selectedFullCoinbaseHex,
				"submit_block_coinbase_txid_wire_le", selectedSubmitCoinbaseTxIDWireLE,
				"submit_block_merkle_root_wire_le", selectedSubmitMerkleRootWireLE,
				"header_merkle_root_wire_le", selectedHeaderMerkleRootWireLE,
				"submit_block_tx_count", selectedSubmitTxCount,
				"submit_block_txids_wire_le", selectedSubmitTxIDsWireLE,
				"submit_block_recomputed_merkle_matches_header", false)
			c.recordShare(c.workerName, false, 0, selectedDiff, "submitblock-assembly-merkle-mismatch", hashHex, now)
			c.sendShareError(submit.ChannelID, submit.SequenceNumber, "submitblock-assembly-merkle-mismatch")
			return
		}
		preflight, err := preflightSubmitBlockRPC(blockHex)
		if err != nil {
			logger.Warn("sv2_submitblock_rpc_preflight_failed", "component", "stratum", "kind", "sv2_submitblock_rpc_preflight_failed",
				"remote", c.id, "worker", c.workerName,
				"sv2_trace", func() string {
					tCtx := c.newSV2SubmitTraceCtx(submit, info)
					tCtx.Ver = selectedVersionHex
					tCtx.NTime = selectedNTimeHex
					tCtx.Nonce = selectedNonceHex
					tCtx.NBits = selectedBitsHex
					tCtx.Prev = selectedPrevHex
					tCtx.ExN = hex.EncodeToString(standardExtranonce)
					tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
					tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
					tCtx.Merkle = selectedMerkleRootDisplayBE
					tCtx.Hdr80 = selectedHeaderHex
					tCtx.Hash = hashHex
					tCtx.Result = "block-rejected"
					tCtx.Reason = "submitblock-rpc-preflight-failed"
					tCtx.Mode = selectedMode
					tCtx.JobType = info.jobType
					return BuildSV2Trace(tCtx)
				}(),
				"hash", hashHex,
				"submit_block_rpc_hex", blockHex,
				"submit_block_rpc_len", len(blockHex),
				"error", err)
			c.recordShare(c.workerName, false, 0, selectedDiff, "submitblock-rpc-preflight-failed", hashHex, now)
			c.sendShareError(submit.ChannelID, submit.SequenceNumber, "submitblock-rpc-preflight-failed")
			return
		}
		logger.Debug("submitblock rpc payload", "component", "stratum", "kind", "submitblock_rpc_payload",
			"remote", c.id, "worker", c.workerName,
			"hash", hashHex,
			"submit_block_rpc_hex", preflight.rpcHex,
			"submit_block_rpc_len", preflight.rpcLen,
			"submit_block_rpc_prefix_hex", preflight.rpcPrefixHex,
			"submit_block_rpc_header80_hex", preflight.rpcHeader80Hex,
			"submit_block_rpc_tx_count_varint_hex", preflight.rpcTxCountVarintHex,
			"submit_block_rpc_first_tx_hex", preflight.rpcFirstTxHex,
			"submit_block_rpc_header_merkle_wire_le", preflight.headerMerkleWireLE,
			"submit_block_rpc_recomputed_merkle_wire_le", preflight.parsedMerkleWireLE,
			"submit_block_rpc_tx_count", preflight.parsedTxCount,
			"submit_block_rpc_txids_wire_le", preflight.parsedTxidsWireLE,
			"submit_block_rpc_recomputed_merkle_matches_header", preflight.merkleMatchesHeader)
		if !preflight.merkleMatchesHeader {
			logger.Warn("sv2_submitblock_rpc_preflight_failed", "component", "stratum", "kind", "sv2_submitblock_rpc_preflight_failed",
				"remote", c.id, "worker", c.workerName,
				"sv2_trace", func() string {
					tCtx := c.newSV2SubmitTraceCtx(submit, info)
					tCtx.Ver = selectedVersionHex
					tCtx.NTime = selectedNTimeHex
					tCtx.Nonce = selectedNonceHex
					tCtx.NBits = selectedBitsHex
					tCtx.Prev = selectedPrevHex
					tCtx.ExN = hex.EncodeToString(standardExtranonce)
					tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
					tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
					tCtx.Merkle = selectedMerkleRootDisplayBE
					tCtx.Hdr80 = selectedHeaderHex
					tCtx.Hash = hashHex
					tCtx.Result = "block-rejected"
					tCtx.Reason = "submitblock-rpc-preflight-merkle-mismatch"
					tCtx.Mode = selectedMode
					tCtx.JobType = info.jobType
					return BuildSV2Trace(tCtx)
				}(),
				"hash", hashHex,
				"submit_block_rpc_hex", preflight.rpcHex,
				"submit_block_rpc_len", preflight.rpcLen,
				"submit_block_rpc_header80_hex", preflight.rpcHeader80Hex,
				"submit_block_rpc_tx_count_varint_hex", preflight.rpcTxCountVarintHex,
				"submit_block_rpc_first_tx_hex", preflight.rpcFirstTxHex,
				"parsed_tx_count", preflight.parsedTxCount,
				"parsed_tx_count_offsets", preflight.parsedTxCountOffsets,
				"parsed_txids_wire_le", preflight.parsedTxidsWireLE,
				"parsed_tx_offsets", preflight.parsedTxOffsets,
				"recomputed_merkle_wire_le", preflight.parsedMerkleWireLE,
				"header_merkle_wire_le", preflight.headerMerkleWireLE,
				"byte_diff_offsets", preflight.merkleDiffOffsets)
			c.recordShare(c.workerName, false, 0, selectedDiff, "submitblock-rpc-preflight-merkle-mismatch", hashHex, now)
			c.sendShareError(submit.ChannelID, submit.SequenceNumber, "submitblock-rpc-preflight-merkle-mismatch")
			return
		}
		rpcBlockHex := preflight.rpcHex
		var submitRes interface{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.rpc.callCtx(ctx, "submitblock", []any{rpcBlockHex}, &submitRes); err != nil {
			if c.metrics != nil {
				c.metrics.RecordBlockSubmission("error")
			}
			logger.Error("sv2 block submit failed", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName, "hash", hashHex, "error", err)
		} else if resultErr := submitBlockResultError((*any)(&submitRes)); resultErr != nil {
			if c.metrics != nil {
				c.metrics.RecordBlockSubmission("error")
			}
			logger.Warn("sv2 block submit rejected", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName, "hash", hashHex,
				"sv2_trace", func() string {
					tCtx := c.newSV2SubmitTraceCtx(submit, info)
					tCtx.Ver = selectedVersionHex
					tCtx.NTime = selectedNTimeHex
					tCtx.Nonce = selectedNonceHex
					tCtx.NBits = selectedBitsHex
					tCtx.Prev = selectedPrevHex
					tCtx.ExN = hex.EncodeToString(standardExtranonce)
					tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
					tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
					tCtx.Merkle = selectedMerkleRootDisplayBE
					tCtx.Hdr80 = selectedHeaderHex
					tCtx.Hash = hashHex
					tCtx.SDiff = sv2FormatFloat(computedShareDiff)
					tCtx.TDiff = sv2FormatFloat(shareThreshold)
					tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
					tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
					tCtx.NetTarget = networkTargetHex
					tCtx.Result = "block-rejected"
					tCtx.Reason = resultErr.Error()
					tCtx.Mode = selectedMode
					tCtx.JobType = info.jobType
					return BuildSV2Trace(tCtx)
				}(),
				"result", resultErr.Error())
		} else {
			if c.metrics != nil {
				c.metrics.RecordBlockSubmission("accepted")
			}
			c.logFoundBlock(job, c.workerName, hashHex, selectedDiff)
			logger.Info("sv2 block submit accepted", "component", "stratum", "kind", "sv2",
				"remote", c.id, "worker", c.workerName, "hash", hashHex,
				"sv2_trace", func() string {
					tCtx := c.newSV2SubmitTraceCtx(submit, info)
					tCtx.Ver = selectedVersionHex
					tCtx.NTime = selectedNTimeHex
					tCtx.Nonce = selectedNonceHex
					tCtx.NBits = selectedBitsHex
					tCtx.Prev = selectedPrevHex
					tCtx.ExN = hex.EncodeToString(standardExtranonce)
					tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
					tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
					tCtx.Merkle = selectedMerkleRootDisplayBE
					tCtx.Hdr80 = selectedHeaderHex
					tCtx.Hash = hashHex
					tCtx.SDiff = sv2FormatFloat(computedShareDiff)
					tCtx.TDiff = sv2FormatFloat(shareThreshold)
					tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
					tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
					tCtx.NetTarget = networkTargetHex
					tCtx.Result = "block-accepted"
					tCtx.Reason = "-"
					tCtx.Mode = selectedMode
					tCtx.JobType = info.jobType
					return BuildSV2Trace(tCtx)
				}())
			blockFound = true
			authoritativeMode = selectedMode
		}
	}
	if isBlock && !blockFound {
		logger.Warn("sv2 reconstruction invalid despite network target", "component", "stratum", "kind", "sv2_reconstruction_invalid",
			"remote", c.id, "worker", c.workerName,
			"sv2_trace", func() string {
				tCtx := c.newSV2SubmitTraceCtx(submit, info)
				tCtx.Ver = selectedVersionHex
				tCtx.NTime = selectedNTimeHex
				tCtx.Nonce = selectedNonceHex
				tCtx.NBits = selectedBitsHex
				tCtx.Prev = selectedPrevHex
				tCtx.ExN = hex.EncodeToString(standardExtranonce)
				tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
				tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
				tCtx.Merkle = selectedMerkleRootDisplayBE
				tCtx.Hdr80 = selectedHeaderHex
				tCtx.Hash = hashHex
				tCtx.Result = "rejected"
				tCtx.Reason = "invalid-reconstruction"
				tCtx.Mode = selectedMode
				tCtx.JobType = info.jobType
				return BuildSV2Trace(tCtx)
			}(),
			"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
			"selected_mode", selectedMode,
			"selected_mode_experimental", selectedModeExperimental,
			"full_header80_hex", selectedHeaderHex,
			"double_sha256_header_be", hashHex,
			"double_sha256_header_le", hex.EncodeToString(headerHashRaw[:]),
			"meets_network_target", true,
			"authoritative_mode", authoritativeMode,
		)
		c.recordShare(c.workerName, false, 0, selectedDiff, "invalidReconstruction", hashHex, now)
		c.sendShareError(submit.ChannelID, submit.SequenceNumber, "invalid-reconstruction")
		return
	}
	logger.Debug("sv2 reconstruction authority", "component", "stratum", "kind", "sv2_reconstruction_authority",
		"remote", c.id, "worker", c.workerName,
		"channel_id", submit.ChannelID, "seq", submit.SequenceNumber, "job_id", submit.JobID,
		"sv2_trace", func() string {
			tCtx := c.newSV2SubmitTraceCtx(submit, info)
			tCtx.Ver = selectedVersionHex
			tCtx.NTime = selectedNTimeHex
			tCtx.Nonce = selectedNonceHex
			tCtx.NBits = selectedBitsHex
			tCtx.Prev = selectedPrevHex
			tCtx.ExN = hex.EncodeToString(standardExtranonce)
			tCtx.ExNLen = fmt.Sprintf("%d", len(standardExtranonce))
			tCtx.CoinbaseTxID = selectedCoinbaseTxIDDisplayBE
			tCtx.Merkle = selectedMerkleRootDisplayBE
			tCtx.Hdr80 = selectedHeaderHex
			tCtx.Hash = hashHex
			tCtx.SDiff = sv2FormatFloat(computedShareDiff)
			tCtx.TDiff = sv2FormatFloat(shareThreshold)
			tCtx.NDiff = sv2FormatFloat(networkDiffBDiff)
			tCtx.ShareTarget = hex.EncodeToString(shareTargetBE[:])
			tCtx.NetTarget = networkTargetHex
			if blockFound {
				tCtx.Result = "block-accepted"
				tCtx.Reason = "-"
			} else if isBlock {
				tCtx.Result = "block-candidate"
				tCtx.Reason = "-"
			} else {
				tCtx.Result = "accepted"
				tCtx.Reason = "-"
			}
			tCtx.Mode = selectedMode
			tCtx.JobType = info.jobType
			return BuildSV2Trace(tCtx)
		}(),
		"selected_mode", selectedMode,
		"authoritative_mode", authoritativeMode,
		"is_block_candidate", isBlock,
		"submitblock_accepted", blockFound)
	// Keep parity with SV1: block-valid submits are still valid shares and
	// should contribute to accepted/share-rate/hashrate accounting.
	c.recordShare(c.workerName, true, shareThreshold, selectedDiff, "", hashHex, now)
	c.maybeUpdateSavedWorkerMinuteBestDiff(selectedDiff, now)
	c.maybeUpdateSavedWorkerBestDiff(selectedDiff)
	if c.metrics != nil {
			c.metrics.TrackBestShare(c.workerName, hashHex, selectedDiff, now)
	}
	if blockFound && logger.Enabled(logLevelInfo) {
		stats := c.snapshotShareInfo(now).Stats
		logger.Info("block found",
			"component", "stratum",
			"kind", "sv2",
			"miner", c.workerName,
			"height", job.Template.Height,
			"hash", hashHex,
			"accepted_total", stats.Accepted,
			"rejected_total", stats.Rejected,
			"worker_total_accepted_diff", stats.TotalDifficulty,
		)
	}

	atomic.StoreUint32(&c.sequenceAck, submit.SequenceNumber)
	c.sendShareSuccess(submit.ChannelID, submit.SequenceNumber)
	c.maybeAdjustDifficulty(now)
}

func (c *sv2Conn) logFoundBlock(job *Job, worker, hashHex string, shareDiff float64) {
	if c == nil || job == nil {
		return
	}
	dir := c.cfg.DataDir
	if dir == "" {
		dir = defaultDataDir
	}
	total := job.Template.CoinbaseValue
	feePct := c.cfg.PoolFeePercent
	if feePct < 0 {
		feePct = 0
	}
	if feePct > 99.99 {
		feePct = 99.99
	}
	poolFee := max(int64(math.Round(float64(total)*feePct/100.0)), 0)
	if poolFee > total {
		poolFee = total
	}
	workerAmt := total - poolFee

	workerAddr := workerBaseAddress(worker)
	if workerAddr == "" || strings.EqualFold(workerAddr, c.cfg.PayoutAddress) {
		poolFee = total
		workerAmt = 0
	}

	rec := map[string]any{
		"timestamp":           time.Now().UTC(),
		"height":              job.Template.Height,
		"hash":                hashHex,
		"worker":              worker,
		"share_diff_bdiff":    shareDiff,
		"job_id":              job.JobID,
		"payout_address":      c.cfg.PayoutAddress,
		"coinbase_value_sats": total,
		"pool_fee_sats":       poolFee,
		"worker_payout_sats":  workerAmt,
	}
	data, err := fastJSONMarshal(rec)
	if err != nil {
		logger.Warn("sv2 found block log marshal", "component", "stratum", "kind", "sv2", "error", err)
		return
	}
	line := append(data, '\n')
	select {
	case foundBlockLogCh <- foundBlockLogEntry{Dir: dir, Line: line}:
	default:
		logger.Warn("sv2 found block log queue full; dropping entry", "component", "stratum", "kind", "sv2")
	}
}

func (c *sv2Conn) isDuplicateShare(jobID uint32, extranonce2 []byte, ntime, nonce uint32, version uint32) bool {
	if !c.cfg.ShareCheckDuplicate {
		return false
	}
	cache := c.shareCache[jobID]
	if cache == nil {
		cache = &duplicateShareSet{}
		c.shareCache[jobID] = cache
	}
	var key duplicateShareKey
	makeDuplicateShareKeyDecoded(&key, extranonce2, ntime, nonce, version)
	return cache.seenOrAdd(key)
}

func (c *sv2Conn) maybeAdjustDifficulty(now time.Time) {
	if c == nil {
		return
	}
	varDiffEnabled := c.cfg.VarDiffEnabled || c.cfg.TargetSharesPerMin <= 0
	if !varDiffEnabled {
		return
	}
	window := c.vardiff.AdjustmentWindow
	if window <= 0 {
		window = defaultVarDiffAdjustmentWindow
	}
	if c.shareWinStart.IsZero() {
		c.shareWinStart = now
		c.shareWinAccept = 1
		return
	}
	c.shareWinAccept++
	elapsed := now.Sub(c.shareWinStart)
	if elapsed < window {
		return
	}
	if c.vardiff.RetargetDelay > 0 && !c.lastRetargetAt.IsZero() && now.Sub(c.lastRetargetAt) < c.vardiff.RetargetDelay {
		return
	}
	if c.shareWinAccept <= 0 || elapsed <= 0 {
		c.shareWinStart = now
		c.shareWinAccept = 0
		return
	}
	targetShares := c.vardiff.TargetSharesPerMin
	if targetShares <= 0 {
		targetShares = defaultVarDiffTargetSharesPerMin
	}
	if targetShares <= 0 {
		targetShares = 6
	}
	actualSharesPerMin := float64(c.shareWinAccept) / elapsed.Minutes()
	if actualSharesPerMin <= 0 {
		c.shareWinStart = now
		c.shareWinAccept = 0
		return
	}
	curDiff := c.difficulty
	targetDiff := curDiff * (actualSharesPerMin / targetShares)
	if targetDiff <= 0 {
		c.shareWinStart = now
		c.shareWinAccept = 0
		return
	}
	damping := c.vardiff.DampingFactor
	if damping <= 0 || damping > 1 {
		damping = defaultVarDiffDampingFactor
	}
	newDiff := curDiff + ((targetDiff - curDiff) * damping)
	newDiff = c.clampDifficultyWithMinerLimits(newDiff)
	if curJob := c.jobMgr.CurrentJob(); curJob != nil {
		newDiff = c.capDifficultyForJob(newDiff, curJob)
	}
	if math.Abs(newDiff-curDiff) < 1e-6 {
		c.shareWinStart = now
		c.shareWinAccept = 0
		return
	}
	if curDiff > 0 {
		ratio := newDiff / curDiff
		if ratio > 0.95 && ratio < 1.05 {
			c.shareWinStart = now
			c.shareWinAccept = 0
			return
		}
	}
	old := curDiff
	prev := c.difficulty
	c.difficulty = newDiff
	c.lastRetargetAt = now
	if err := c.sendSetTarget(); err != nil {
		c.difficulty = old
		logger.Warn("sv2 set-target send failed", "component", "stratum", "kind", "sv2", "remote", c.id, "error", err)
		return
	}
	c.previousDifficulty = prev
	c.lastDiffChangeSV2 = now
	if c.metrics != nil {
		dir := "down"
		if newDiff > old {
			dir = "up"
		}
		c.metrics.RecordVardiffMove(dir)
	}
	logger.Info("sv2 vardiff adjust", "component", "stratum", "kind", "sv2", "remote", c.id,
		"worker", c.workerName, "old_diff", old, "new_diff", newDiff,
		"window_shares", c.shareWinAccept, "window_sec", elapsed.Seconds())
	c.shareWinStart = now
	c.shareWinAccept = 0
}

func (c *sv2Conn) sendSetTarget() error {
	diff := c.difficulty
	if curJob := c.jobMgr.CurrentJob(); curJob != nil {
		diff = c.capDifficultyForJob(diff, curJob)
	}
	msg := sv2SetTarget{ChannelID: c.channelID, MaximumTarget: c.targetForDifficulty(diff)}
	if err := c.writeFrame(sv2MsgSetTarget, msg.encode()); err != nil {
		return err
	}
	c.activeTarget = msg.MaximumTarget
	if shareValidationDebugEnabled() {
		logger.Debug("share validation debug",
			"component", "stratum",
			"kind", "sv2_set_target",
			"stratum_version", "sv2",
			"connection_id", c.id,
			"worker_name", c.workerName,
			"channel_id", c.channelID,
			"set_target", hex.EncodeToString(msg.MaximumTarget[:]),
			"required_difficulty", diff,
		)
	}
	return nil
}

func clampDifficultyToRange(diff, minDiff, maxDiff float64) float64 {
	if minDiff > 0 && diff < minDiff {
		diff = minDiff
	}
	if maxDiff > 0 && diff > maxDiff {
		diff = maxDiff
	}
	if diff <= 0 {
		return defaultMinDifficulty
	}
	return diff
}

func (c *sv2Conn) sendShareSuccess(channelID, seqNum uint32) {
	msg := sv2SubmitSharesSuccess{
		ChannelID:               channelID,
		LastSequenceNumber:      seqNum,
		NewSubmitsAcceptedCount: 1,
		NewSharesSum:            1,
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
