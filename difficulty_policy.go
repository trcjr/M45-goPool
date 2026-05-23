package main

import (
	"fmt"
	"math"
)

func capShareDifficultyByNetwork(requestedDiff, networkDiff float64) (effectiveShareDiff float64, capped bool, networkValid bool) {
	effectiveShareDiff = requestedDiff
	if networkDiff <= 0 || math.IsNaN(networkDiff) || math.IsInf(networkDiff, 0) {
		return effectiveShareDiff, false, false
	}
	if requestedDiff <= 0 || math.IsNaN(requestedDiff) || math.IsInf(requestedDiff, 0) {
		return effectiveShareDiff, false, true
	}
	if requestedDiff > networkDiff {
		return networkDiff, true, true
	}
	return requestedDiff, false, true
}

func networkDifficultyFromJob(job *Job) (float64, bool, error) {
	if job == nil {
		return 0, false, nil
	}
	bitsU32, err := parseUint32BEHexPadded(job.Template.Bits)
	if err != nil {
		return 0, false, fmt.Errorf("parse job bits %q: %w", job.Template.Bits, err)
	}
	networkDiff := difficultyFromBits(bitsU32)
	if networkDiff <= 0 || math.IsNaN(networkDiff) || math.IsInf(networkDiff, 0) {
		return 0, false, fmt.Errorf("invalid network difficulty from bits %q: %g", job.Template.Bits, networkDiff)
	}
	return networkDiff, true, nil
}
