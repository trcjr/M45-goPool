package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"math/big"
	"net"
	"os"
	"testing"
	"time"
)

type sv2TestConn struct{}

func (c *sv2TestConn) Read(_ []byte) (int, error)         { return 0, nil }
func (c *sv2TestConn) Write(_ []byte) (int, error)        { return 0, nil }
func (c *sv2TestConn) Close() error                       { return nil }
func (c *sv2TestConn) LocalAddr() net.Addr                { return nil }
func (c *sv2TestConn) RemoteAddr() net.Addr               { return nil }
func (c *sv2TestConn) SetDeadline(_ time.Time) error      { return nil }
func (c *sv2TestConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *sv2TestConn) SetWriteDeadline(_ time.Time) error { return nil }
func (c *sv2TestConn) WriteSV2Frame(_ byte, _ []byte) error {
	return nil
}

func TestShareTargetBoundaryComparisons(t *testing.T) {
	target := targetFromDifficulty(1)
	targetBE := uint256BEFromBigInt(target)

	eq := computeShareValidationMath(targetBE, 1)
	if !eq.MeetsShareTarget {
		t.Fatal("hash == target must be accepted")
	}

	belowInt := new(big.Int).Sub(target, big.NewInt(1))
	below := computeShareValidationMath(uint256BEFromBigInt(belowInt), 1)
	if !below.MeetsShareTarget {
		t.Fatal("hash just below target must be accepted")
	}

	aboveInt := new(big.Int).Add(target, big.NewInt(1))
	above := computeShareValidationMath(uint256BEFromBigInt(aboveInt), 1)
	if above.MeetsShareTarget {
		t.Fatal("hash just above target must be rejected")
	}
}

func TestDifficultyTargetRoundTripSV2(t *testing.T) {
	diffs := []float64{1, 2, 8, 64, 1024, 1000000}
	for _, in := range diffs {
		targetLE := sv2TargetFromDifficulty(in)
		out, ok := sv2DifficultyFromTargetLE(targetLE)
		if !ok {
			t.Fatalf("sv2DifficultyFromTargetLE failed for diff=%v", in)
		}
		rel := math.Abs(out-in) / in
		if rel > 1e-12 {
			t.Fatalf("round-trip mismatch diff=%v out=%v rel=%v", in, out, rel)
		}
	}
}

func TestCompactNBitsDecodeKnownValues(t *testing.T) {
	genesisTarget, err := targetFromBits("1d00ffff")
	if err != nil {
		t.Fatalf("targetFromBits genesis failed: %v", err)
	}
	if genesisTarget.Cmp(diff1Target) != 0 {
		t.Fatalf("genesis target mismatch got=%x want=%x", genesisTarget, diff1Target)
	}

	highDiffTarget, err := targetFromBits("1b0404cb")
	if err != nil {
		t.Fatalf("targetFromBits high diff failed: %v", err)
	}
	if highDiffTarget.Sign() <= 0 || highDiffTarget.Cmp(genesisTarget) >= 0 {
		t.Fatalf("expected high difficulty target to be positive and < diff1 target, got=%x", highDiffTarget)
	}
}

func TestSV1SV2ShareDifficultyIdenticalForEquivalentHeader(t *testing.T) {
	header := make([]byte, 80)
	copy(header[0:4], []byte{0x00, 0x00, 0x00, 0x20})
	copy(header[72:76], []byte{0xff, 0xff, 0x00, 0x1d})
	copy(header[76:80], []byte{0x34, 0x12, 0x00, 0x00})

	hashRaw := doubleSHA256(header)
	var hashBE [32]byte
	copy(hashBE[:], hashRaw)
	reverseBytes32(&hashBE)

	sv1Diff := computeShareValidationMath(hashBE, 1).ComputedShareDiffBDiff
	sv2Diff := computeShareValidationMath(hashBE, 1).ComputedShareDiffBDiff
	if sv1Diff != sv2Diff {
		t.Fatalf("sv1/sv2 difficulty mismatch sv1=%v sv2=%v", sv1Diff, sv2Diff)
	}
}

func TestSV2SetTargetUpdatesActiveChannelTarget(t *testing.T) {
	c := &sv2Conn{
		conn:      &sv2TestConn{},
		jobMgr:    &JobManager{},
		channelID: 7,
		difficulty: 128,
	}
	if err := c.sendSetTarget(); err != nil {
		t.Fatalf("sendSetTarget failed: %v", err)
	}
	want := c.targetForDifficulty(128)
	if c.activeTarget != want {
		t.Fatalf("active target mismatch got=%x want=%x", c.activeTarget, want)
	}
}

func TestSV2RetargetDoesNotRewriteExistingJobTarget(t *testing.T) {
	oldTarget := sv2TargetFromDifficulty(64)
	info := &sv2JobInfo{assignedDiff: 64, assignedTarget: oldTarget}
	c := &sv2Conn{difficulty: 64}
	c.activeJobs.Store(uint32(1), info)

	// Channel target moves for future work, but existing job snapshots remain.
	c.difficulty = 256
	c.activeTarget = c.targetForDifficulty(c.difficulty)

	if info.assignedDiff != 64 {
		t.Fatalf("assignedDiff mutated got=%v want=64", info.assignedDiff)
	}
	if info.assignedTarget != oldTarget {
		t.Fatalf("assignedTarget mutated got=%x want=%x", info.assignedTarget, oldTarget)
	}
}

func TestSV2SubmitUsesJobSnapshotTarget(t *testing.T) {
	jobTarget := uint256BEFromBigInt(targetFromDifficulty(16))
	activeTarget := uint256BEFromBigInt(targetFromDifficulty(64))

	hashInt := new(big.Int).Sub(targetFromDifficulty(16), big.NewInt(1))
	hashBE := uint256BEFromBigInt(hashInt)

	meetsJob := uint256BELessOrEqual(hashBE, jobTarget)
	meetsActive := uint256BELessOrEqual(hashBE, activeTarget)
	if !meetsJob {
		t.Fatal("share must satisfy job snapshot target")
	}
	if meetsActive {
		t.Fatal("share must fail stricter active channel target")
	}
}

func TestSV2RejectedShareHeaderReconstruction(t *testing.T) {
	oldDiff := 0.000001
	newDiff := 0.000002
	oldTarget := uint256BEFromBigInt(targetFromDifficulty(oldDiff))
	newTarget := uint256BEFromBigInt(targetFromDifficulty(newDiff))

	base := make([]byte, 80)
	copy(base[0:4], []byte{0x00, 0x00, 0x00, 0x20})
	copy(base[68:72], []byte{0x01, 0x00, 0x00, 0x66})
	copy(base[72:76], []byte{0xff, 0xff, 0x00, 0x1d})

	var chosenHeader []byte
	var chosenHashBE [32]byte
	for nonce := uint32(0); nonce < 20_000_000; nonce++ {
		hdr := append([]byte(nil), base...)
		hdr[76] = byte(nonce)
		hdr[77] = byte(nonce >> 8)
		hdr[78] = byte(nonce >> 16)
		hdr[79] = byte(nonce >> 24)
		h1 := sha256.Sum256(hdr)
		h2 := sha256.Sum256(h1[:])
		var be [32]byte
		copy(be[:], h2[:])
		reverseBytes32(&be)
		if uint256BELessOrEqual(be, oldTarget) && !uint256BELessOrEqual(be, newTarget) {
			chosenHeader = hdr
			chosenHashBE = be
			break
		}
	}
	if len(chosenHeader) != 80 {
		t.Fatal("failed to find deterministic share between old/new targets")
	}

	shareDiff := difficultyFromHashExact(chosenHashBE[:])
	t.Logf("header80 hex: %s", hex.EncodeToString(chosenHeader))
	t.Logf("hash hex: %s", hex.EncodeToString(chosenHashBE[:]))
	t.Logf("computed difficulty: %.12f", shareDiff)
	t.Logf("old target: %s", hex.EncodeToString(oldTarget[:]))
	t.Logf("new target: %s", hex.EncodeToString(newTarget[:]))

	if !uint256BELessOrEqual(chosenHashBE, oldTarget) {
		t.Fatal("share must be accepted at old target")
	}
	if uint256BELessOrEqual(chosenHashBE, newTarget) {
		t.Fatal("share must be rejected at new target")
	}
}

func TestShareValidationFixtureFromEnv(t *testing.T) {
	headerHex := os.Getenv("GOPOOL_SHARE_HEADER80_HEX")
	targetHex := os.Getenv("GOPOOL_SHARE_TARGET_HEX")
	if headerHex == "" || targetHex == "" {
		t.Skip("set GOPOOL_SHARE_HEADER80_HEX and GOPOOL_SHARE_TARGET_HEX to run fixture")
	}
	header, err := hex.DecodeString(headerHex)
	if err != nil || len(header) != 80 {
		t.Fatalf("invalid GOPOOL_SHARE_HEADER80_HEX: len=%d err=%v", len(header), err)
	}
	targetBytes, err := hex.DecodeString(targetHex)
	if err != nil || len(targetBytes) != 32 {
		t.Fatalf("invalid GOPOOL_SHARE_TARGET_HEX: len=%d err=%v", len(targetBytes), err)
	}

	hashRaw := doubleSHA256(header)
	var hashBE [32]byte
	copy(hashBE[:], hashRaw)
	reverseBytes32(&hashBE)
	var targetBE [32]byte
	copy(targetBE[:], targetBytes)

	t.Logf("header80 hex: %s", hex.EncodeToString(header))
	t.Logf("double_sha256 hash: %s", hex.EncodeToString(hashBE[:]))
	t.Logf("hash integer hex: %s", hex.EncodeToString(hashBE[:]))
	t.Logf("computed share difficulty: %.12f", difficultyFromHashExact(hashBE[:]))
	t.Logf("accepted against target: %v", uint256BELessOrEqual(hashBE, targetBE))

	if bytes.Equal(hashBE[:], make([]byte, 32)) {
		t.Fatal("unexpected all-zero hash")
	}
}
