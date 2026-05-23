package main

import (
	"bytes"
	"context"
	"errors"
	"encoding/hex"
	"fmt"
	"time"
)

type submitBlockRPCPreflight struct {
	rpcHex                        string
	rpcLen                        int
	rpcPrefixHex                  string
	rpcHeader80Hex                string
	rpcTxCountVarintHex           string
	rpcFirstTxHex                 string
	headerMerkleWireLE            string
	parsedMerkleWireLE            string
	parsedTxCount                 uint64
	parsedTxidsWireLE             []string
	parsedTxCountOffsets          []int
	parsedTxOffsets               [][2]int
	merkleDiffOffsets             []int
	merkleMatchesHeader           bool
	blockBytes                    []byte
	parsedHeader80                []byte
}

func preflightSubmitBlockRPC(blockHex string) (*submitBlockRPCPreflight, error) {
	raw, err := hex.DecodeString(blockHex)
	if err != nil {
		return nil, fmt.Errorf("decode submitblock hex: %w", err)
	}
	if len(raw) < 81 {
		return nil, fmt.Errorf("submitblock payload too short: %d bytes", len(raw))
	}
	if len(raw) < 80 {
		return nil, fmt.Errorf("submitblock header too short: %d bytes", len(raw))
	}
	head := make([]byte, 80)
	copy(head, raw[:80])
	txCount, txCountLen, err := readVarInt(raw[80:])
	if err != nil {
		return nil, fmt.Errorf("parse tx count: %w", err)
	}
	offset := 80 + txCountLen
	if txCount == 0 {
		return nil, fmt.Errorf("submitblock tx count must be at least 1")
	}
	parsedTxids := make([][32]byte, 0, txCount)
	parsedTxidsWire := make([]string, 0, txCount)
	parsedTxOffsets := make([][2]int, 0, txCount)
	var firstTxHex string
	for i := uint64(0); i < txCount; i++ {
		txStart := offset
		if txStart >= len(raw) {
			return nil, fmt.Errorf("tx %d truncated at offset %d", i, txStart)
		}
		txBytes, txid, consumed, err := parseSerializedTxForSubmitBlock(raw[txStart:])
		if err != nil {
			return nil, fmt.Errorf("parse tx %d: %w", i, err)
		}
		txEnd := txStart + consumed
		parsedTxOffsets = append(parsedTxOffsets, [2]int{txStart, txEnd})
		if i == 0 {
			firstTxHex = hex.EncodeToString(txBytes)
		}
		parsedTxids = append(parsedTxids, txid)
		parsedTxidsWire = append(parsedTxidsWire, hex.EncodeToString(txid[:]))
		offset = txEnd
	}
	if offset != len(raw) {
		return nil, fmt.Errorf("unexpected trailing submitblock bytes: %d", len(raw)-offset)
	}
	recomputedMerkle, ok := sv2ComputeMerkleRootFromTxIDs(parsedTxids)
	if !ok {
		return nil, fmt.Errorf("recompute merkle from submitblock txs failed")
	}
	headMerkle := raw[36:68]
	merkleMatches := bytes.Equal(recomputedMerkle[:], headMerkle)
	diffOffsets := diffHeaderHexByteOffsets(hex.EncodeToString(headMerkle), hex.EncodeToString(recomputedMerkle[:]), 32)
	return &submitBlockRPCPreflight{
		rpcHex:               blockHex,
		rpcLen:               len(blockHex),
		rpcPrefixHex:         hex.EncodeToString(raw[:minIntSubmitBlock(len(raw), 128)]),
		rpcHeader80Hex:       hex.EncodeToString(head),
		rpcTxCountVarintHex:  hex.EncodeToString(raw[80 : 80+txCountLen]),
		rpcFirstTxHex:        firstTxHex,
		headerMerkleWireLE:   hex.EncodeToString(headMerkle),
		parsedMerkleWireLE:   hex.EncodeToString(recomputedMerkle[:]),
		parsedTxCount:        txCount,
		parsedTxidsWireLE:    parsedTxidsWire,
		parsedTxCountOffsets: []int{80, 80 + txCountLen},
		parsedTxOffsets:      parsedTxOffsets,
		merkleDiffOffsets:    diffOffsets,
		merkleMatchesHeader:  merkleMatches,
		blockBytes:           raw,
		parsedHeader80:       head,
	}, nil
}

func minIntSubmitBlock(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// submitBlockWithFastRetry aggressively retries submitblock without backoff
// to maximize the chance of winning the propagation race. It retries every
// 100ms until either submitblock succeeds, a newer job height is observed,
// or a safety window elapses.
func (mc *MinerConn) submitBlockWithFastRetry(job *Job, workerName, hashHex, blockHex string, submitRes *any) error {
	const (
		retryInterval = 100 * time.Millisecond
		// rpcCallTimeout bounds each individual RPC call so a hung bitcoind
		// doesn't block the retry loop indefinitely.
		rpcCallTimeout = 5 * time.Second
		// confirmTimeout bounds getblockheader checks used to detect cases where
		// submitblock may have succeeded but the RPC response timed out.
		confirmTimeout = 2 * time.Second
		// maxRetryWindow is a final safety cap; in practice we expect to
		// stop much sooner when a new block is seen. Using a full block
		// interval keeps us racing hard for rare finds.
		maxRetryWindow = 10 * time.Minute
	)

	start := time.Now()
	attempt := 0
	var lastErr error
	preflight, err := preflightSubmitBlockRPC(blockHex)
	if err != nil {
		logger.Warn("sv2_submitblock_rpc_preflight_failed", "component", "stratum", "kind", "sv2_submitblock_rpc_preflight_failed",
			"worker", mc.minerName(workerName),
			"hash", hashHex,
			"submit_block_rpc_hex", blockHex,
			"submit_block_rpc_len", len(blockHex),
			"error", err)
		return err
	}
	logger.Debug("submitblock rpc payload", "component", "stratum", "kind", "submitblock_rpc_payload",
		"worker", mc.minerName(workerName),
		"hash", hashHex,
		"submit_block_rpc_hex", preflight.rpcHex,
		"submit_block_rpc_len", preflight.rpcLen,
		"submit_block_rpc_prefix_hex", preflight.rpcPrefixHex,
		"submit_block_rpc_header80_hex", preflight.rpcHeader80Hex,
		"submit_block_rpc_tx_count_varint_hex", preflight.rpcTxCountVarintHex,
		"submit_block_rpc_first_tx_hex", preflight.rpcFirstTxHex,
		"submit_block_rpc_header_merkle_wire_le", preflight.headerMerkleWireLE,
		"submit_block_rpc_recomputed_merkle_wire_le", preflight.parsedMerkleWireLE,
		"submit_block_rpc_tx_count", preflight.parsedTxCount,
		"submit_block_rpc_txids_wire_le", preflight.parsedTxidsWireLE,
		"submit_block_rpc_recomputed_merkle_matches_header", preflight.merkleMatchesHeader)
	if !preflight.merkleMatchesHeader {
		logger.Warn("sv2_submitblock_rpc_preflight_failed", "component", "stratum", "kind", "sv2_submitblock_rpc_preflight_failed",
			"worker", mc.minerName(workerName),
			"hash", hashHex,
			"submit_block_rpc_hex", preflight.rpcHex,
			"submit_block_rpc_len", preflight.rpcLen,
			"submit_block_rpc_header80_hex", preflight.rpcHeader80Hex,
			"submit_block_rpc_tx_count_varint_hex", preflight.rpcTxCountVarintHex,
			"submit_block_rpc_first_tx_hex", preflight.rpcFirstTxHex,
			"parsed_tx_count", preflight.parsedTxCount,
			"parsed_tx_count_offsets", preflight.parsedTxCountOffsets,
			"parsed_txids_wire_le", preflight.parsedTxidsWireLE,
			"parsed_tx_offsets", preflight.parsedTxOffsets,
			"recomputed_merkle_wire_le", preflight.parsedMerkleWireLE,
			"header_merkle_wire_le", preflight.headerMerkleWireLE,
			"byte_diff_offsets", preflight.merkleDiffOffsets)
		return fmt.Errorf("submitblock preflight: merkle mismatch")
	}
	rpcBlockHex := preflight.rpcHex

	blockAccepted := func() bool {
		if mc.rpc == nil || hashHex == "" {
			return false
		}
		// If bitcoind is overloaded, it's possible for submitblock to
		// succeed server-side while our client-side context times out.
		// Confirm by checking whether the block hash is now known.
		var header struct {
			Confirmations int64 `json:"confirmations"`
		}
		ctx, cancel := context.WithTimeout(context.Background(), confirmTimeout)
		err := mc.rpc.callCtx(ctx, "getblockheader", []any{hashHex, true}, &header)
		cancel()
		// Only treat as success when the block is in the best chain.
		// (Orphaned blocks can still be "known" but will have confirmations=-1.)
		return err == nil && header.Confirmations >= 1
	}

	for {
		attempt++

		// Use a per-call timeout to prevent indefinite hangs on unresponsive RPC.
		// The retry loop continues regardless; we just don't want one call to block forever.
		callCtx, cancel := context.WithTimeout(context.Background(), rpcCallTimeout)
		err := mc.rpc.callCtx(callCtx, "submitblock", []any{rpcBlockHex}, submitRes)
		cancel()

		if err == nil {
			if resultErr := submitBlockResultError(submitRes); resultErr != nil {
				if blockAccepted() {
					logger.Warn("submitblock returned rejection but block is in chain; treating as success",
						"attempts", attempt,
						"worker", mc.minerName(workerName),
						"hash", hashHex,
						"result", resultErr.Error(),
					)
					return nil
				}
				return resultErr
			}
			if attempt > 1 {
				logger.Info("submitblock succeeded after retries",
					"attempts", attempt,
					"worker", mc.minerName(workerName),
					"hash", hashHex,
				)
			}
			return nil
		}
		lastErr = err

		// If submitblock timed out client-side, check whether the block was
		// accepted anyway. This commonly happens when bitcoind's RPC work
		// queue is saturated.
		if errors.Is(err, context.DeadlineExceeded) && blockAccepted() {
			logger.Warn("submitblock timed out but block is in chain; treating as success",
				"attempts", attempt,
				"worker", mc.minerName(workerName),
				"hash", hashHex,
			)
			return nil
		}

		// Log the first failure loudly; subsequent failures are summarized
		// when we eventually give up.
		if attempt == 1 {
			logger.Error("submitblock error; retrying aggressively",
				"error", err,
				"worker", mc.minerName(workerName),
				"hash", hashHex,
			)
		}

		// If we've already seen a newer template height, there's no point
		// continuing to spam submitblock for this block.
		if mc.jobMgr != nil && job != nil {
			if cur := mc.jobMgr.CurrentJob(); cur != nil && cur.Template.Height > job.Template.Height {
				logger.Warn("submitblock giving up after new block seen",
					"original_height", job.Template.Height,
					"current_height", cur.Template.Height,
					"attempts", attempt,
					"error", err,
				)
				return err
			}
		}

		// Safety stop: avoid spinning forever if the node is persistently
		// unreachable or rejects the block.
		if time.Since(start) >= maxRetryWindow {
			logger.Error("submitblock giving up after retry window",
				"attempts", attempt,
				"duration", time.Since(start),
				"error", lastErr,
			)
			return lastErr
		}

		time.Sleep(retryInterval)
	}
}

func submitBlockResultError(submitRes *any) error {
	if submitRes == nil || *submitRes == nil {
		return nil
	}
	switch v := (*submitRes).(type) {
	case string:
		if v == "" {
			return nil
		}
		return fmt.Errorf("submitblock rejected: %s", v)
	default:
		return fmt.Errorf("submitblock returned unexpected result %T: %v", *submitRes, *submitRes)
	}
}
