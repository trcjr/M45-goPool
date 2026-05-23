package main

import (
	"math"
	"testing"
	"time"
)

func TestSV2SuggestedStartDiffFromNominalHashrate(t *testing.T) {
	c := &sv2Conn{vardiff: VarDiffConfig{MinDiff: 1, MaxDiff: 4096, TargetSharesPerMin: 15}}

	// At diff 1, expected shares/min = hashrate*60/hashPerShare.
	// Choose 15 shares/min so the suggested diff should be near 1.
	nominalAtDiff1 := (hashPerShare * 15.0) / 60.0
	diff, ok := c.suggestedStartDiffFromNominalHashrate(nominalAtDiff1)
	if !ok {
		t.Fatal("expected nominal hashrate to produce a suggested diff")
	}
	if diff < 0.99 || diff > 1.01 {
		t.Fatalf("diff=%v want approximately 1", diff)
	}

	// 10x hashrate should imply roughly 10x difficulty before clamping.
	diff, ok = c.suggestedStartDiffFromNominalHashrate(nominalAtDiff1 * 10.0)
	if !ok {
		t.Fatal("expected high nominal hashrate to produce a suggested diff")
	}
	if diff < 9.9 || diff > 10.1 {
		t.Fatalf("diff=%v want approximately 10", diff)
	}
}

func TestSV2SuggestedStartDiffFromNominalHashrateClamps(t *testing.T) {
	c := &sv2Conn{vardiff: VarDiffConfig{MinDiff: 8, MaxDiff: 64, TargetSharesPerMin: 15}}
	nominalAtDiff1 := (hashPerShare * 15.0) / 60.0

	diff, ok := c.suggestedStartDiffFromNominalHashrate(nominalAtDiff1)
	if !ok {
		t.Fatal("expected nominal hashrate to produce a suggested diff")
	}
	if diff != 8 {
		t.Fatalf("diff=%v want min clamp 8", diff)
	}

	diff, ok = c.suggestedStartDiffFromNominalHashrate(nominalAtDiff1 * 500.0)
	if !ok {
		t.Fatal("expected very high nominal hashrate to produce a suggested diff")
	}
	if diff != 64 {
		t.Fatalf("diff=%v want max clamp 64", diff)
	}
}

func TestSV2SuggestedStartDiffHonorsMinerMaxTargetFloor(t *testing.T) {
	maxTarget := sv2TargetFromDifficulty(8)
	minDiff, ok := sv2DifficultyFromTargetLE(maxTarget)
	if !ok {
		t.Fatal("expected max target to produce minimum difficulty floor")
	}

	c := &sv2Conn{
		vardiff:           VarDiffConfig{MinDiff: 1, MaxDiff: 1024, TargetSharesPerMin: 15},
		hasMinerMaxTarget: true,
		minerMaxTarget:    maxTarget,
		minerMinDiff:      minDiff,
	}

	nominalAtDiff1 := (hashPerShare * 15.0) / 60.0
	diff, got := c.suggestedStartDiffFromNominalHashrate(nominalAtDiff1)
	if !got {
		t.Fatal("expected startup difficulty suggestion")
	}
	if math.Abs(diff-minDiff) > 1e-6 {
		t.Fatalf("diff=%v want miner min diff floor %v", diff, minDiff)
	}
}

func TestSV2TargetForDifficultyClampsToMinerMaxTarget(t *testing.T) {
	maxTarget := sv2TargetFromDifficulty(16)
	c := &sv2Conn{
		hasMinerMaxTarget: true,
		minerMaxTarget:    maxTarget,
	}

	// Lower difficulty than diff=16 would produce a larger (easier) target,
	// which must be clamped to miner max_target.
	got := c.targetForDifficulty(1)
	if got != maxTarget {
		t.Fatalf("got target %x want clamped %x", got, maxTarget)
	}
}

func TestSV2MeetsPrevDiffGrace(t *testing.T) {
	now := time.Now()
	c := &sv2Conn{
		previousDifficulty: 64,
		lastDiffChangeSV2:  now.Add(-5 * time.Second),
	}
	if !c.meetsPrevDiffGrace(63, now) {
		t.Fatal("expected previous-difficulty grace to accept near-threshold share")
	}
	if c.meetsPrevDiffGrace(1, now.Add(previousDiffGracePeriod+time.Second)) {
		t.Fatal("expected previous-difficulty grace to expire")
	}
}
