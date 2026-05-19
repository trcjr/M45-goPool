package main

import (
	"encoding/binary"
	"encoding/hex"
	"math/big"
)

type sv2JobInfo struct {
	job    *Job
	coinb1 string // hex
	coinb2 string // hex
}

// sv2BuildNewMiningJob builds a NewMiningJob message from a Job.
// CbPrefix = decoded coinb1 (includes extranonce1), CbSuffix = decoded coinb2.
func sv2BuildNewMiningJob(info *sv2JobInfo, channelID, jobID uint32, futureJob bool) *sv2NewMiningJob {
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

	return &sv2NewMiningJob{
		ChannelID:             channelID,
		JobID:                 jobID,
		FutureJob:             futureJob,
		Version:               uint32(job.Template.Version),
		VersionRollingAllowed: job.VersionMask != 0,
		MerklePath:            merklePath,
		CbPrefix:              cbPrefix,
		CbSuffix:              cbSuffix,
	}
}

// sv2BuildSetNewPrevHash builds a SetNewPrevHash message from a Job.
func sv2BuildSetNewPrevHash(job *Job, channelID, jobID uint32) *sv2SetNewPrevHash {
	prevHash := job.prevHashBytes
	nbits := binary.LittleEndian.Uint32(job.bitsBytes[:])

	return &sv2SetNewPrevHash{
		ChannelID: channelID,
		JobID:     jobID,
		PrevHash:  prevHash,
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
