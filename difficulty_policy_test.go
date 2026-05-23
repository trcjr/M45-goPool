package main

import (
	"math"
	"sync"
	"testing"
)

func TestCapShareDifficultyByNetwork(t *testing.T) {
	tests := []struct {
		name        string
		requested   float64
		network     float64
		want        float64
		wantCapped  bool
		wantNetValid bool
	}{
		{
			name:        "mainnet-like requested below network",
			requested:   6984,
			network:     100000000000000,
			want:        6984,
			wantCapped:  false,
			wantNetValid: true,
		},
		{
			name:        "regtest-like requested above network",
			requested:   6984,
			network:     0.000000000466,
			want:        0.000000000466,
			wantCapped:  true,
			wantNetValid: true,
		},
		{
			name:        "requested already below network",
			requested:   32,
			network:     128,
			want:        32,
			wantCapped:  false,
			wantNetValid: true,
		},
		{
			name:        "invalid network diff fallback",
			requested:   64,
			network:     math.NaN(),
			want:        64,
			wantCapped:  false,
			wantNetValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, capped, netValid := capShareDifficultyByNetwork(tc.requested, tc.network)
			if math.Abs(got-tc.want) > 1e-15 {
				t.Fatalf("effective diff = %.16g want %.16g", got, tc.want)
			}
			if capped != tc.wantCapped {
				t.Fatalf("capped = %v want %v", capped, tc.wantCapped)
			}
			if netValid != tc.wantNetValid {
				t.Fatalf("networkValid = %v want %v", netValid, tc.wantNetValid)
			}
		})
	}
}

func TestSV1AssignedDifficultyCappedByNetwork(t *testing.T) {
	mc := &MinerConn{
		jobDifficulty:          make(map[string]float64, 1),
		jobRequestedDifficulty: make(map[string]float64, 1),
	}
	job := &Job{Template: GetBlockTemplateResult{Bits: "207fffff"}}
	requested := 6984.0
	effective := mc.capDifficultyForJob(requested, job, "worker.test")
	mc.setJobDifficulty("job-1", effective, requested)

	networkDiff, ok, err := networkDifficultyFromJob(job)
	if err != nil || !ok {
		t.Fatalf("networkDifficultyFromJob failed: ok=%v err=%v", ok, err)
	}
	if got := mc.assignedDifficulty("job-1"); math.Abs(got-networkDiff) > 1e-18 {
		t.Fatalf("assigned difficulty = %.16g want %.16g", got, networkDiff)
	}
	if got := mc.requestedDifficultyForJob("job-1"); math.Abs(got-requested) > 1e-12 {
		t.Fatalf("requested difficulty = %.16g want %.16g", got, requested)
	}
}

func TestSV2SetAssignedDiffOnActiveJobsCapsByNetwork(t *testing.T) {
	c := &sv2Conn{}
	job := &Job{Template: GetBlockTemplateResult{Bits: "207fffff"}}
	info := &sv2JobInfo{job: job}
	c.activeJobs = sync.Map{}
	c.activeJobs.Store(uint32(1), info)

	requested := 6984.0
	c.setAssignedDiffOnActiveJobs(requested)

	networkDiff, ok, err := networkDifficultyFromJob(job)
	if err != nil || !ok {
		t.Fatalf("networkDifficultyFromJob failed: ok=%v err=%v", ok, err)
	}
	if math.Abs(info.requestedDiff-requested) > 1e-12 {
		t.Fatalf("requestedDiff = %.16g want %.16g", info.requestedDiff, requested)
	}
	if math.Abs(info.assignedDiff-networkDiff) > 1e-18 {
		t.Fatalf("assignedDiff = %.16g want %.16g", info.assignedDiff, networkDiff)
	}
}
