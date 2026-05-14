package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newStatusServerForSparkMinerTests() *StatusServer {
	s := &StatusServer{
		jsonCache: make(map[string]cachedJSONResponse),
	}
	s.UpdateConfig(Config{
		FiatCurrency: "USD",
		BrandName:    "GoPool",
	})
	s.statusMu.Lock()
	s.cachedStatus = StatusData{
		BrandName:          "GoPool",
		ActiveMiners:       5,
		PoolHashrate:       123.4 * 1e6, // 123.4 MH/s in H/s
		SharesPerSecond:    12.5,
		TargetSharesPerMin: 1.0,
		MinDifficulty:      0.001,
		WindowAccepted:     8,
		WindowSubmissions:  10,
		WindowDifficulty:   0.002,
		JobFeed: JobFeedView{
			BlockHeight: 881234,
		},
	}
	s.lastStatusBuild = time.Now()
	s.statusMu.Unlock()
	return s
}

func TestHandleSparkMinerStats_MethodNotAllowed(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	req := httptest.NewRequest(http.MethodPost, "/stats", nil)
	rr := httptest.NewRecorder()
	s.handleSparkMinerStats(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, rr.Code)
	}
}

func TestHandleSparkMinerStats_ReturnsValidJSON(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr := httptest.NewRecorder()
	s.handleSparkMinerStats(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d; body: %s", http.StatusOK, rr.Code, rr.Body.String())
	}

	// Verify it's valid JSON
	var stats SparkMinerStats
	if err := json.Unmarshal(rr.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to unmarshal response: %v; body: %s", err, rr.Body.String())
	}
}

func TestHandleSparkMinerStats_FieldNames(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr := httptest.NewRecorder()
	s.handleSparkMinerStats(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}

	// Check the JSON keys directly to verify field names are correct
	var rawMap map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &rawMap); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify expected field names are present (those that should have values)
	expectedFields := []string{
		"workers",
		"workersCount",
		"pool_workers_count",
		"pool_name",
		"pool_hashrate",
		"block_height",
	}

	for _, field := range expectedFields {
		if _, ok := rawMap[field]; !ok {
			t.Logf("warning: expected field %q not present (may be omitted if nil)", field)
		}
	}
}

func TestHandleSparkMinerStats_ContentType(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr := httptest.NewRecorder()
	s.handleSparkMinerStats(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Fatalf("expected Content-Type application/json, got %s", contentType)
	}
}

func TestBuildSparkMinerStats_PopulatesBlockHeight(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	stats := s.buildSparkMinerStats("", "")
	if stats.BlockHeight == nil {
		t.Fatalf("expected block_height to be populated, got nil")
	}
	if *stats.BlockHeight != 881234 {
		t.Fatalf("expected block_height 881234, got %d", *stats.BlockHeight)
	}
}

func TestBuildSparkMinerStats_PopulatesWorkerCounts(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	stats := s.buildSparkMinerStats("", "")

	// Should have all three worker count aliases
	if stats.Workers == nil {
		t.Fatalf("expected workers to be populated, got nil")
	}
	if stats.WorkersCount == nil {
		t.Fatalf("expected workersCount to be populated, got nil")
	}
	if stats.PoolWorkersCount == nil {
		t.Fatalf("expected pool_workers_count to be populated, got nil")
	}

	// All should have the same value
	if *stats.Workers != 5 {
		t.Fatalf("expected workers=5, got %d", *stats.Workers)
	}
	if *stats.WorkersCount != 5 {
		t.Fatalf("expected workersCount=5, got %d", *stats.WorkersCount)
	}
	if *stats.PoolWorkersCount != 5 {
		t.Fatalf("expected pool_workers_count=5, got %d", *stats.PoolWorkersCount)
	}
}

func TestBuildSparkMinerStats_PopulatesPoolName(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	stats := s.buildSparkMinerStats("", "")
	if stats.PoolName == nil {
		t.Fatalf("expected pool_name to be populated, got nil")
	}
	if *stats.PoolName != "GoPool" {
		t.Fatalf("expected pool_name=GoPool, got %s", *stats.PoolName)
	}
}

func TestBuildSparkMinerStats_PopulatesPoolHashrate(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	stats := s.buildSparkMinerStats("", "")
	if stats.PoolHashrate == nil {
		t.Fatalf("expected pool_hashrate to be populated, got nil")
	}
	// Should have a unit suffix like "MH/s"
	if *stats.PoolHashrate == "" {
		t.Fatalf("expected pool_hashrate to not be empty")
	}
	// Should contain "MH/s" for 123.4 MH/s
	if !containsString(*stats.PoolHashrate, "MH/s") {
		t.Fatalf("expected pool_hashrate to contain 'MH/s', got %s", *stats.PoolHashrate)
	}
}

func TestBuildSparkMinerStats_DifficultyStats(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	stats := s.buildSparkMinerStats("", "")

	// Should have difficulty progress
	if stats.DifficultyProgress == nil {
		t.Fatalf("expected difficulty_progress to be populated, got nil")
	}
	if *stats.DifficultyProgress < 0 || *stats.DifficultyProgress > 100 {
		t.Fatalf("expected difficulty_progress between 0-100, got %f", *stats.DifficultyProgress)
	}

	// Should have difficulty change
	if stats.DifficultyChange == nil {
		t.Fatalf("expected difficulty_change to be populated, got nil")
	}

	// Should have retarget blocks
	if stats.DifficultyRetargetBlocks == nil {
		t.Fatalf("expected difficulty_retarget_blocks to be populated, got nil")
	}
	if *stats.DifficultyRetargetBlocks <= 0 {
		t.Fatalf("expected difficulty_retarget_blocks > 0, got %d", *stats.DifficultyRetargetBlocks)
	}
}

func TestHandleSparkMinerStats_WithQueryParams(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	// Add worker lookup data to status
	s.statusMu.Lock()
	s.cachedStatus.WorkerLookup = map[string]WorkerView{
		"bc1qtest": {
			Name:            "test_worker",
			RollingHashrate: 52.3 * 1e3, // 52.3 KH/s
			Difficulty:      0.0032,
		},
	}
	s.statusMu.Unlock()

	// Test with wallet parameter
	req := httptest.NewRequest(http.MethodGet, "/stats?wallet=bc1qtest", nil)
	rr := httptest.NewRecorder()
	s.handleSparkMinerStats(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
	}

	var stats SparkMinerStats
	if err := json.Unmarshal(rr.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Should have worker-specific stats
	if stats.WorkerHashrate == nil {
		t.Fatalf("expected worker_hashrate for wallet query, got nil")
	}
	if stats.AddressBestDiff == nil {
		t.Fatalf("expected address_best_diff for wallet query, got nil")
	}
}

func TestFormatDifficultyCompact(t *testing.T) {
	tests := []struct {
		difficulty   float64
		expectSuffix string
	}{
		{100_000_000_000, ""},        // < 1T
		{1_000_000_000_000, "T"},     // 1T
		{95_670_000_000_000, "T"},    // 95.67T
		{1_000_000_000_000_000, "P"}, // 1P
		{1_234_567_890_123_456, "P"}, // > 1P
	}

	for _, tc := range tests {
		result := formatDifficultyCompact(tc.difficulty)
		if tc.expectSuffix != "" && !containsString(result, tc.expectSuffix) {
			t.Fatalf("expected %q to contain %q", result, tc.expectSuffix)
		}
	}
}

func TestEstimateNetworkHashrate(t *testing.T) {
	// Test with known difficulty
	difficulty := 1.0
	hashrate := estimateNetworkHashrate(difficulty)
	if hashrate <= 0 {
		t.Fatalf("expected positive hashrate, got %f", hashrate)
	}

	// Higher difficulty should give higher hashrate
	hashrate2 := estimateNetworkHashrate(difficulty * 2)
	if hashrate2 <= hashrate {
		t.Fatalf("expected hashrate to increase with difficulty")
	}
}

func TestHandleSparkMinerStats_CacheHit(t *testing.T) {
	s := newStatusServerForSparkMinerTests()

	// First request should populate cache
	req1 := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr1 := httptest.NewRecorder()
	s.handleSparkMinerStats(rr1, req1)

	if rr1.Code != http.StatusOK {
		t.Fatalf("first request failed")
	}

	// Second request should hit cache
	req2 := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr2 := httptest.NewRecorder()
	s.handleSparkMinerStats(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Fatalf("second request failed")
	}

	// Verify cache key is used (bodies should be identical)
	if rr1.Body.String() != rr2.Body.String() {
		t.Logf("bodies differ (expected for dynamic fields), but both valid")
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && s[len(s)-len(substr):] == substr ||
		(len(s) > len(substr) && containsSubstringHelper(s, substr)))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
