package main

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/big"
)

type sv2JobInfo struct {
	job          *Job
	coinb1       string // hex
	coinb2       string // hex
	channelType  string
	jobType      string
	expectsMerkleReconstruction bool
	standardMerkleRoot [32]byte
	standardCoinbaseTx []byte
	standardExtranonce []byte
	requestedDiff float64
	assignedDiff float64
	assignedTarget [32]byte
}

// sv2BuildNewExtendedMiningJob builds a NewExtendedMiningJob message from a Job.
// CbPrefix/CbSuffix come from the pre-built coinbase split where miners insert
// negotiated extranonce bytes between them.
func sv2BuildNewExtendedMiningJob(info *sv2JobInfo, channelID, jobID uint32) *sv2NewExtendedMiningJob {
	job := info.job

	cbPrefix, _ := hex.DecodeString(info.coinb1)
	cbSuffix, _ := hex.DecodeString(info.coinb2)

	merklePath := make([][32]byte, len(job.MerkleBranches))
	for i, branch := range job.MerkleBranches {
		b, _ := hex.DecodeString(branch)
		if len(b) == 32 {
			copy(merklePath[i][:], b)
		}
	}

	minNTime := uint32(job.Template.Mintime)

	return &sv2NewExtendedMiningJob{
		ChannelID:             channelID,
		JobID:                 jobID,
		MinNTime:              &minNTime,
		Version:               uint32(job.Template.Version),
		VersionRollingAllowed: job.VersionMask != 0,
		MerklePath:            merklePath,
		CbPrefix:              cbPrefix,
		CbSuffix:              cbSuffix,
	}
}

// sv2BuildNewStandardMiningJob builds a NewMiningJob message for standard channels.
func sv2BuildNewStandardMiningJob(job *Job, channelID, jobID uint32, merkleRoot [32]byte) *sv2NewMiningJob {
	if job == nil {
		return nil
	}
	minNTime := uint32(job.Template.Mintime)
	return &sv2NewMiningJob{
		ChannelID:  channelID,
		JobID:      jobID,
		MinNTime:   &minNTime,
		Version:    uint32(job.Template.Version),
		MerkleRoot: merkleRoot,
	}
}

// sv2BuildSetNewPrevHash builds a SetNewPrevHash message from a Job.
func sv2BuildSetNewPrevHash(job *Job, channelID, jobID uint32) *sv2SetNewPrevHash {
	// job.prevHashBytes is decoded from bitcoind's display-format hex (big-endian).
	// SV2 SetNewPrevHash.prev_hash must carry the prevhash in internal/little-endian
	// byte order — the same bytes that appear at offset 4-35 of a Bitcoin block
	// header.  Reverse the display-format bytes before sending.
	var prevHashLE [32]byte
	for i := 0; i < 32; i++ {
		prevHashLE[i] = job.prevHashBytes[31-i]
	}
	// job.bitsBytes are decoded from big-endian hex (e.g. "1d00ffff").
	// SV2 u32 fields are serialized little-endian by sv2AppendU32, so keep
	// the numeric compact value in host order here.
	nbits := binary.BigEndian.Uint32(job.bitsBytes[:])

	return &sv2SetNewPrevHash{
		ChannelID: channelID,
		JobID:     jobID,
		PrevHash:  prevHashLE,
		MinNTime:  uint32(job.Template.Mintime),
		NBits:     nbits,
	}
}

// sv2TargetFromDifficulty converts a difficulty to a 32-byte SV2 target (LE).
func sv2TargetFromDifficulty(diff float64) [32]byte {
	bigTarget := targetFromDifficulty(diff)
	return sv2TargetFromBigInt(bigTarget)
}

func sv2TargetFromBigInt(bigTarget *big.Int) [32]byte {
	be := uint256BEFromBigInt(bigTarget) // [32]byte big-endian
	// Reverse for little-endian
	var le [32]byte
	for i, b := range be {
		le[31-i] = b
	}
	return le
}

func sv2TargetLEToBigInt(target [32]byte) *big.Int {
	var be [32]byte
	for i, b := range target {
		be[31-i] = b
	}
	return new(big.Int).SetBytes(be[:])
}

func sv2DifficultyFromTargetLE(target [32]byte) (float64, bool) {
	bigTarget := sv2TargetLEToBigInt(target)
	if bigTarget == nil || bigTarget.Sign() <= 0 {
		return 0, false
	}
	diff1 := new(big.Float).SetPrec(256).SetInt(diff1Target)
	tgt := new(big.Float).SetPrec(256).SetInt(bigTarget)
	if tgt.Sign() <= 0 {
		return 0, false
	}
	out := new(big.Float).Quo(diff1, tgt)
	diff, _ := out.Float64()
	if diff <= 0 || math.IsNaN(diff) || math.IsInf(diff, 0) {
		return 0, false
	}
	return diff, true
}

func sv2TargetIsZero(target [32]byte) bool {
	for _, b := range target {
		if b != 0 {
			return false
		}
	}
	return true
}

// sv2TargetGreaterThan reports whether LE target a is numerically greater than b.
func sv2TargetGreaterThan(a, b [32]byte) bool {
	for i := len(a) - 1; i >= 0; i-- {
		if a[i] > b[i] {
			return true
		}
		if a[i] < b[i] {
			return false
		}
	}
	return false
}
