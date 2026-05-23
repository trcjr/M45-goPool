package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"time"
)

func (mc *MinerConn) processSubmissionTask(task submissionTask) {
	start := task.receivedAt
	if start.IsZero() {
		start = time.Now()
	}
	defer func() {
		mc.recordSubmitRTT(time.Since(start))
	}()

	workerName := task.workerName
	jobID := task.jobID
	extranonce2 := task.extranonce2
	ntime := task.ntime
	nonce := task.nonce
	versionHex := task.versionHex

	if debugLogging || verboseRuntimeLogging {
		logger.Debug("submit received",
			"component", "miner",
			"kind", "submit",
			"remote", mc.id,
			"worker", workerName,
			"job", jobID,
			"extranonce2", extranonce2,
			"ntime", ntime,
			"nonce", nonce,
			"version", versionHex,
		)
	}

	ctx, ok := mc.prepareShareContext(task)
	if !ok {
		return
	}
	mc.processShare(task, ctx)
}

func (mc *MinerConn) processShare(task submissionTask, ctx shareContext) {
	job := task.job
	workerName := task.workerName
	jobID := task.jobID
	policyReject := task.policyReject
	reqID := task.reqID
	now := task.receivedAt
	extranonce2 := task.extranonce2
	ntime := task.ntime
	nonce := task.nonce
	versionHex := task.versionHex

	assignedDiff := task.assignedDifficulty
	if assignedDiff <= 0 {
		assignedDiff = mc.assignedDifficulty(jobID)
	}
	uncappedRequestedDiff := mc.requestedDifficultyForJob(jobID)
	if uncappedRequestedDiff <= 0 {
		uncappedRequestedDiff = assignedDiff
	}
	currentDiff := mc.currentDifficulty()
	creditedDiff := assignedDiff
	if creditedDiff <= 0 {
		creditedDiff = currentDiff
	}

	thresholdDiff := assignedDiff
	if thresholdDiff <= 0 {
		thresholdDiff = currentDiff
	}

	if task.hasAlternateVersion {
		altTask := task
		altTask.useVersion = task.alternateUseVersion
		altTask.versionHex = task.alternateVersionHex
		if altCtx, ok := mc.prepareShareContext(altTask); ok {
			primaryAcceptable := mc.submissionMeetsAssignedDifficulty(ctx, thresholdDiff, now)
			alternateAcceptable := mc.submissionMeetsAssignedDifficulty(altCtx, thresholdDiff, now)
			if preferAlternateSubmissionContext(ctx, altCtx, primaryAcceptable, alternateAcceptable) {
				task = altTask
				ctx = altCtx
				policyReject = task.policyReject
				versionHex = task.versionHex
			}
		}
	}

	if !ctx.isBlock && policyReject.reason != rejectUnknown {
		mc.rejectShareWithBan(&StratumRequest{ID: reqID, Method: "mining.submit"}, workerName, policyReject.reason, policyReject.errCode, policyReject.errMsg, now)
		return
	}

	if !ctx.isBlock && mc.cfg.ShareCheckDuplicate && mc.isDuplicateShare(jobID, (&task).extranonce2Decoded(), task.ntimeVal, task.nonceVal, task.useVersion) {
		ex2Log := extranonce2
		if ex2Log == "" {
			ex2Log = hex.EncodeToString((&task).extranonce2Decoded())
		}
		ntimeLog := ntime
		if ntimeLog == "" {
			ntimeLog = uint32ToHex8Lower(task.ntimeVal)
		}
		nonceLog := nonce
		if nonceLog == "" {
			nonceLog = uint32ToHex8Lower(task.nonceVal)
		}
		verLog := versionHex
		if verLog == "" {
			verLog = uint32ToHex8Lower(task.useVersion)
		}
		logger.Info("duplicate share",
			"component", "miner",
			"kind", "reject",
			"remote", mc.id,
			"job", jobID,
			"extranonce2", ex2Log,
			"ntime", ntimeLog,
			"nonce", nonceLog,
			"version", verLog,
		)
		mc.rejectShareWithBan(&StratumRequest{ID: reqID, Method: "mining.submit"}, workerName, rejectDuplicateShare, stratumErrCodeDuplicateShare, "duplicate share", now)
		return
	}

	validationMath := computeShareValidationMath(ctx.headerHashLE, thresholdDiff)
	lowDiff := !ctx.isBlock && !validationMath.MeetsShareTarget
	shareTargetBE := validationMath.ShareTargetBE
	networkTargetBE := job.targetBE
	if networkTargetBE == ([32]byte{}) && job.Target != nil && job.Target.Sign() > 0 {
		networkTargetBE = uint256BEFromBigInt(job.Target)
	}
	meetsShareTarget := validationMath.MeetsShareTarget
	meetsNetworkTarget := networkTargetBE != ([32]byte{}) && uint256BELessOrEqual(ctx.headerHashLE, networkTargetBE)
	computedShareDiff := validationMath.ComputedShareDiffBDiff
	networkDiffBDiff := 0.0
	if bitsU32, err := parseUint32BEHexPadded(job.Template.Bits); err == nil {
		networkDiffBDiff = difficultyFromBits(bitsU32)
	}
	logger.Debug("share validation",
		"component", "stratum",
		"kind", "sv1",
		"connection_worker", mc.currentWorker(),
		"submit_worker", workerName,
		"job_id", jobID,
		"header_hash_display_be", ctx.hashHex,
		"header_hash_wire_le", hex.EncodeToString(ctx.headerHashBE[:]),
		"header_hash_compare_be", hex.EncodeToString(ctx.headerHashLE[:]),
		"header_hex", hex.EncodeToString(ctx.header),
		"share_target_be", hex.EncodeToString(shareTargetBE[:]),
		"network_target_be", hex.EncodeToString(networkTargetBE[:]),
		"comparison_basis", "be",
		"share_compare_hash_uint256_be", hex.EncodeToString(ctx.headerHashLE[:]),
		"share_compare_target_uint256_be", hex.EncodeToString(shareTargetBE[:]),
		"network_compare_hash_uint256_be", hex.EncodeToString(ctx.headerHashLE[:]),
		"network_compare_target_uint256_be", hex.EncodeToString(networkTargetBE[:]),
		"assigned_share_diff_bdiff", thresholdDiff,
		"uncapped_requested_share_diff_bdiff", uncappedRequestedDiff,
		"computed_share_diff_bdiff", computedShareDiff,
		"network_diff_bdiff", networkDiffBDiff,
		"meets_share_target", meetsShareTarget,
		"meets_network_target", meetsNetworkTarget,
		"share_compare", "header_hash <= share_target",
		"network_compare", "header_hash <= network_target",
		"is_block", ctx.isBlock,
		"version", uint32ToHex8Lower(task.useVersion),
		"ntime", uint32ToHex8Lower(task.ntimeVal),
		"nonce", uint32ToHex8Lower(task.nonceVal),
		"nbits", job.Template.Bits,
		"prevhash_be", job.Template.Previous,
		"merkle_root_be", hex.EncodeToString(ctx.merkleRoot),
		"extranonce1", hex.EncodeToString(mc.extranonce1),
		"extranonce2", hex.EncodeToString((&task).extranonce2Decoded()),
	)
	if (debugLogging || verboseRuntimeLogging) && computedShareDiff > 0 {
		delta := math.Abs(computedShareDiff - ctx.shareDiff)
		if delta/computedShareDiff > 1e-3 {
			logger.Warn("difficulty consistency mismatch",
				"component", "stratum",
				"kind", "sv1",
				"job_id", jobID,
				"computed_share_diff_bdiff", computedShareDiff,
				"legacy_share_diff_value", ctx.shareDiff,
				"delta", delta,
			)
		}
	}
	if shareValidationDebugEnabled() {
		validationResult := "accepted"
		rejectReason := ""
		if lowDiff {
			validationResult = "rejected"
			rejectReason = "low-difficulty"
		}
		logger.Debug("share validation debug",
			"component", "stratum",
			"kind", "sv1",
			"stratum_version", "sv1",
			"connection_id", mc.id,
			"remote", mc.id,
			"worker_name", workerName,
			"channel_id", "",
			"job_id", jobID,
			"nonce", uint32ToHex8Lower(task.nonceVal),
			"ntime", uint32ToHex8Lower(task.ntimeVal),
			"version", uint32ToHex8Lower(task.useVersion),
			"extranonce", hex.EncodeToString(append(append([]byte(nil), mc.extranonce1...), (&task).extranonce2Decoded()...)),
			"extranonce2", hex.EncodeToString((&task).extranonce2Decoded()),
			"prevhash", job.Template.Previous,
			"merkle_root", hex.EncodeToString(ctx.merkleRoot),
			"nbits", job.Template.Bits,
			"header80_hex", hex.EncodeToString(ctx.header),
			"hash_hex", ctx.hashHex,
			"hash_interpreted_integer_hex", hex.EncodeToString(ctx.headerHashLE[:]),
			"assigned_target_hex", hex.EncodeToString(shareTargetBE[:]),
			"required_difficulty", thresholdDiff,
			"computed_share_difficulty", computedShareDiff,
			"validation_result", validationResult,
			"reject_reason", rejectReason,
		)
	}
	if lowDiff {
		if debugLogging || verboseRuntimeLogging {
			logger.Debug("share rejected",
				"component", "miner",
				"kind", "reject",
				"computed_share_diff_bdiff", ctx.shareDiff,
				"required_share_diff_bdiff", thresholdDiff,
				"assigned_share_diff_bdiff", assignedDiff,
				"uncapped_requested_share_diff_bdiff", uncappedRequestedDiff,
				"current_share_diff_bdiff", currentDiff,
			)
			logger.Debug("submit rejected: lowDiff",
				"component", "miner",
				"kind", "reject",
				"miner", mc.minerName(workerName),
				"hash", ctx.hashHex,
			)
		}
		var detail *ShareDetail
		if debugLogging || verboseRuntimeLogging {
			detail = mc.buildShareDetailFromCoinbase(job, ctx.cbTx)
		}
		acceptedForStats := false
		mc.recordShare(workerName, acceptedForStats, 0, ctx.shareDiff, "lowDiff", ctx.hashHex, detail, now)

		if banned, invalids := mc.noteInvalidSubmit(now, rejectLowDiff); banned {
			mc.logBan(rejectLowDiff.String(), workerName, invalids)
			mc.writeResponse(StratumResponse{ID: reqID, Result: false, Error: mc.bannedStratumError()})
		} else {
			mc.writeResponse(StratumResponse{
				ID:     reqID,
				Result: false,
				Error:  []any{stratumErrCodeLowDiffShare, fmt.Sprintf("low difficulty share (%.6g expected %.6g)", ctx.shareDiff, thresholdDiff), nil},
			})
		}
		return
	}

	shareHash := ctx.hashHex
	var detail *ShareDetail
	if debugLogging || verboseRuntimeLogging {
		detail = mc.buildShareDetailFromCoinbase(job, ctx.cbTx)
	}

	if ctx.isBlock {
		mc.noteValidSubmit(now)
		// Block-valid submits are still valid shares and must count toward
		// accepted/share-difficulty accounting so hashrate tracking remains
		// accurate on easy networks where most valid submits are blocks.
		mc.recordShare(workerName, true, creditedDiff, ctx.shareDiff, "", shareHash, detail, now)
		mc.handleBlockShare(reqID, job, task.jobID, workerName, (&task).extranonce2Decoded(), uint32ToHex8Lower(task.ntimeVal), uint32ToHex8Lower(task.nonceVal), task.useVersion, task.scriptTime, ctx.hashHex, ctx.shareDiff, now)
		mc.trackBestShare(workerName, shareHash, ctx.shareDiff, now)
		mc.maybeUpdateSavedWorkerMinuteBestDiff(ctx.shareDiff, now)
		mc.maybeUpdateSavedWorkerBestDiff(ctx.shareDiff)
		return
	}

	mc.noteValidSubmit(now)
	mc.recordShare(workerName, true, creditedDiff, ctx.shareDiff, "", shareHash, detail, now)

	// Respond first; any vardiff adjustment and follow-up notify can happen after
	// the submit is acknowledged to minimize perceived submit latency.
	mc.writeTrueResponse(reqID)

	mc.trackBestShare(workerName, shareHash, ctx.shareDiff, now)
	mc.maybeUpdateSavedWorkerMinuteBestDiff(ctx.shareDiff, now)
	mc.maybeUpdateSavedWorkerBestDiff(ctx.shareDiff)

	if mc.maybeAdjustDifficulty(now) {
		mc.sendNotifyFor(job, true)
	}

	if (debugLogging || verboseRuntimeLogging) && logger.Enabled(logLevelInfo) {
		stats, accRate, subRate := mc.snapshotStatsWithRates(now)
		miner := stats.Worker
		if miner == "" {
			miner = workerName
			if miner == "" {
				miner = mc.id
			}
		}
		logger.Debug("share accepted",
			"component", "miner",
			"kind", "share",
			"miner", miner,
			"computed_share_diff_bdiff", ctx.shareDiff,
			"hash", ctx.hashHex,
			"accepted_total", stats.Accepted,
			"rejected_total", stats.Rejected,
			"worker_total_accepted_diff", stats.TotalDifficulty,
			"accept_rate_per_min", accRate,
			"submit_rate_per_min", subRate,
		)
	}
}

func (mc *MinerConn) submissionMeetsAssignedDifficulty(ctx shareContext, thresholdDiff float64, now time.Time) bool {
	_ = now
	if ctx.isBlock {
		return true
	}
	if thresholdDiff <= 0 {
		return true
	}
	return computeShareValidationMath(ctx.headerHashLE, thresholdDiff).MeetsShareTarget
}

func preferAlternateSubmissionContext(primaryCtx, alternateCtx shareContext, primaryAcceptable, alternateAcceptable bool) bool {
	if alternateCtx.isBlock && !primaryCtx.isBlock {
		return true
	}
	if primaryCtx.isBlock {
		return false
	}
	return !primaryAcceptable && alternateAcceptable
}
