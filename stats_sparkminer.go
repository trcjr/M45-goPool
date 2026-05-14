package main

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bytedance/sonic"
)

// SparkMinerStats is the response JSON for the SparkMiner /stats endpoint.
// Fields are omitted if unavailable to keep the response small and ESP32-friendly.
type SparkMinerStats struct {
	// BTC and blockchain data
	BTCPriceUSD *float64 `json:"btc_price_usd,omitempty"`
	BlockHeight *int64   `json:"block_height,omitempty"`

	// Network stats
	NetworkHashrate   *string `json:"network_hashrate,omitempty"`   // e.g., "789.5 EH/s"
	NetworkDifficulty *string `json:"network_difficulty,omitempty"` // e.g., "95.67T"

	// Mempool fees (in sats/vB)
	FeeHalfHour *int `json:"fee_half_hour,omitempty"`
	FeeFastest  *int `json:"fee_fastest,omitempty"`
	FeeHour     *int `json:"fee_hour,omitempty"`

	// External stats cache age in seconds (oldest primary source)
	ExternalStatsAge *int `json:"external_stats_age,omitempty"`

	// Worker counts (including aliases for compatibility)
	Workers          *int    `json:"workers,omitempty"`
	WorkersCount     *int    `json:"workersCount,omitempty"`
	PoolWorkersCount *int    `json:"pool_workers_count,omitempty"`
	PoolName         *string `json:"pool_name,omitempty"`

	// Pool and per-worker stats
	PoolHashrate    *string `json:"pool_hashrate,omitempty"`     // e.g., "123.4 MH/s"
	WorkerHashrate  *string `json:"worker_hashrate,omitempty"`   // e.g., "52.3 KH/s"
	AddressBestDiff *string `json:"address_best_diff,omitempty"` // e.g., "0.0032"

	// Difficulty adjustment (vardiff window stats)
	DifficultyProgress       *float64 `json:"difficulty_progress,omitempty"`        // 0-100, % window complete
	DifficultyChange         *float64 `json:"difficulty_change,omitempty"`          // % change
	DifficultyRetargetBlocks *int64   `json:"difficulty_retarget_blocks,omitempty"` // blocks left
}

// handleSparkMinerStats returns unified pool stats for SparkMiner devices.
func (s *StatusServer) handleSparkMinerStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse optional query parameters
	wallet := strings.TrimSpace(r.URL.Query().Get("wallet"))
	worker := strings.TrimSpace(r.URL.Query().Get("worker"))

	// Build cache key based on parameters
	cacheKey := "sparkminer_stats"
	if wallet != "" {
		cacheKey += "_wallet_" + wallet
		if worker != "" {
			cacheKey += "_worker_" + worker
		}
	}

	// Use aggressive caching to ensure fast response
	cacheTTL := 15 * time.Second // Conservative default
	s.serveCachedJSON(w, cacheKey, cacheTTL, func() ([]byte, error) {
		stats := s.buildSparkMinerStats(wallet, worker)
		return sonic.Marshal(stats)
	})
}

// buildSparkMinerStats constructs the SparkMiner stats response.
func (s *StatusServer) buildSparkMinerStats(wallet, worker string) *SparkMinerStats {
	stats := &SparkMinerStats{}
	now := time.Now()

	// Get current view of pool status
	view := s.statusDataView()

	// 1. BTC Price
	if s.priceSvc != nil {
		fiatCurrency := strings.TrimSpace(s.Config().FiatCurrency)
		if fiatCurrency == "" {
			fiatCurrency = defaultFiatCurrency
		}
		if price, err := s.priceSvc.BTCPrice(fiatCurrency); err == nil && price > 0 {
			stats.BTCPriceUSD = &price
		}
	}

	// 2. Block height
	if view.JobFeed.BlockHeight > 0 {
		stats.BlockHeight = &view.JobFeed.BlockHeight
	}

	// 3. Network hashrate and difficulty (from job feed)
	if s.jobMgr != nil {
		fs := s.jobMgr.FeedStatus()
		blockTip := fs.Payload.BlockTip

		// Network difficulty
		if blockTip.Difficulty > 0 {
			diffStr := formatDifficultyCompact(blockTip.Difficulty)
			stats.NetworkDifficulty = &diffStr
		}

		// Network hashrate (estimated from difficulty and block time)
		if blockTip.Difficulty > 0 {
			// Bitcoin: avg 10 minute block time = 600 seconds
			// hashrate = difficulty * 2^32 / 600
			networkHashrate := estimateNetworkHashrate(blockTip.Difficulty)
			hashrateStr := formatHashrateValue(networkHashrate)
			stats.NetworkHashrate = &hashrateStr
		}
	}

	// 4. Mempool fees (fetch from external service if available)
	fees := s.getMempoolFees()
	if fees != nil {
		stats.FeeHalfHour = fees.HalfHour
		stats.FeeFastest = fees.Fastest
		stats.FeeHour = fees.Hour
		stats.ExternalStatsAge = fees.Age
	}

	// 5. Worker counts
	workerCount := view.ActiveMiners
	if workerCount > 0 {
		stats.Workers = &workerCount
		stats.WorkersCount = &workerCount
		stats.PoolWorkersCount = &workerCount
	}

	// 6. Pool name
	poolName := view.BrandName
	if poolName != "" {
		stats.PoolName = &poolName
	}

	// 7. Pool hashrate
	if view.PoolHashrate > 0 {
		hashrateStr := formatHashrateValue(view.PoolHashrate)
		stats.PoolHashrate = &hashrateStr
	}

	// 8. Per-worker stats (if wallet/worker provided)
	if wallet != "" {
		// Look up worker in the registry
		if s.registry != nil {
			entry, ok := view.WorkerLookup[wallet]
			if ok {
				// Worker hashrate
				if entry.RollingHashrate > 0 {
					hashrateStr := formatHashrateValue(entry.RollingHashrate)
					stats.WorkerHashrate = &hashrateStr
				}

				// Best difficulty
				if entry.Difficulty > 0 {
					diffStr := formatDiffValue(entry.Difficulty)
					stats.AddressBestDiff = &diffStr
				}
			}
		}
	}

	// 9. Difficulty adjustment (vardiff) stats
	if view.WindowAccepted > 0 || view.WindowSubmissions > 0 {
		// Calculate progress: how many shares accepted vs target window
		targetSharesPerMin := view.TargetSharesPerMin
		if targetSharesPerMin <= 0 {
			targetSharesPerMin = 1 // default to avoid division by zero
		}

		// Estimate window size (typically 8 shares)
		targetWindowShares := int64(8)

		// Calculate progress percentage
		progress := float64(view.WindowAccepted) / float64(targetWindowShares) * 100
		if progress > 100 {
			progress = 100
		}
		stats.DifficultyProgress = &progress

		// Difficulty change (previous vs current difficulty)
		// This is a simplified estimate based on window performance
		// In a real implementation, this would come from VarDiff calculation
		if view.WindowDifficulty > 0 && view.MinDifficulty > 0 {
			change := ((view.WindowDifficulty - view.MinDifficulty) / view.MinDifficulty) * 100
			stats.DifficultyChange = &change
		}

		// Difficulty retarget blocks (estimate based on share rate)
		// Bitcoin adjusts difficulty every 2016 blocks; estimate remaining
		if view.SharesPerSecond > 0 {
			secondsPerBlock := 600.0 // Bitcoin target
			secondsPerShare := 1.0 / view.SharesPerSecond
			sharesPerBlock := secondsPerBlock / secondsPerShare
			blocksUntilAdjustment := int64(2016.0 / sharesPerBlock)
			if blocksUntilAdjustment > 0 && blocksUntilAdjustment < 2016 {
				stats.DifficultyRetargetBlocks = &blocksUntilAdjustment
			}
		}
	}

	return stats
}

// MempoolFees holds fee data from external mempool source.
type MempoolFees struct {
	HalfHour *int // sats/vB
	Fastest  *int // sats/vB
	Hour     *int // sats/vB
	Age      *int // seconds since last fetch
}

// getMempoolFees retrieves cached mempool fee data (stubbed for now).
// TODO: Implement actual mempool.space integration with caching.
func (s *StatusServer) getMempoolFees() *MempoolFees {
	// For now, return nil (fees unavailable)
	// This will be populated by a background service that fetches from mempool.space
	return nil
}

// formatDifficultyCompact formats difficulty with T/P suffix for large values.
// Examples: "95.67T", "1.23P"
func formatDifficultyCompact(d float64) string {
	if d <= 0 || d < 1_000_000_000_000 {
		// If < 1 trillion, just use the full number
		return fmt.Sprintf("%.0f", d)
	}

	switch {
	case d >= 1_000_000_000_000_000:
		return fmt.Sprintf("%.2fP", d/1_000_000_000_000_000.0)
	case d >= 1_000_000_000_000:
		return fmt.Sprintf("%.2fT", d/1_000_000_000_000.0)
	default:
		return fmt.Sprintf("%.0f", d)
	}
}

// estimateNetworkHashrate estimates hashrate from network difficulty.
// Bitcoin: hashrate = difficulty * 2^32 / 600 (H/s)
func estimateNetworkHashrate(difficulty float64) float64 {
	const (
		difficulty1Hashrate = 4295032833.0 // 2^32 / 600 (approximate)
	)
	return difficulty * difficulty1Hashrate
}

const defaultFiatCurrency = "usd"
