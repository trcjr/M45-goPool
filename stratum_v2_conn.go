package main

import (
	"bufio"
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
	if len(raw) == 0 {
		return zero, fmt.Errorf("empty tx")
	}
	return doubleSHA256Array(raw), nil
}

func shareValidationDebugEnabled() bool {
	return false
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
	_ = job
	// PR2 boundary: network-difficulty share capping is deferred to PR3.
	return requestedDiff
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
	_ = extranonce
	_ = minerHeader80
	logger.Warn("sv2 submit path deferred", "component", "stratum", "kind", "sv2",
		"remote", c.id, "channel_id", submit.ChannelID, "seq", submit.SequenceNumber,
		"job_id", submit.JobID, "reason", "submit-processing-not-enabled-in-pr2")
	c.sendShareError(submit.ChannelID, submit.SequenceNumber, "unsupported-feature")
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
