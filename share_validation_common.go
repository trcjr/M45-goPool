package main

import (
	"os"
	"strings"
	"sync"
)

var (
	shareValidationDebugOnce sync.Once
	shareValidationDebugFlag bool
)

type shareValidationMath struct {
	ShareTargetBE          [32]byte
	MeetsShareTarget       bool
	ComputedShareDiffBDiff float64
}

func computeShareValidationMath(headerHashCompareBE [32]byte, requiredDiff float64) shareValidationMath {
	if requiredDiff <= 0 {
		return shareValidationMath{
			ShareTargetBE:          uint256BEFromBigInt(maxUint256),
			MeetsShareTarget:       true,
			ComputedShareDiffBDiff: difficultyFromHashExact(headerHashCompareBE[:]),
		}
	}
	shareTargetBE := uint256BEFromBigInt(targetFromDifficulty(requiredDiff))
	return shareValidationMath{
		ShareTargetBE:          shareTargetBE,
		MeetsShareTarget:       uint256BELessOrEqual(headerHashCompareBE, shareTargetBE),
		ComputedShareDiffBDiff: difficultyFromHashExact(headerHashCompareBE[:]),
	}
}

func shareValidationDebugEnabled() bool {
	shareValidationDebugOnce.Do(func() {
		switch strings.ToLower(strings.TrimSpace(os.Getenv("GOPOOL_DEBUG_SHARE_VALIDATION"))) {
		case "1", "true", "yes", "on":
			shareValidationDebugFlag = true
		}
	})
	return shareValidationDebugFlag
}
