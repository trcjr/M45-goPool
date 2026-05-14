package main

import (
	"context"
	"fmt"
	"time"
)

const acceptedBlockRefreshPollInterval = 100 * time.Millisecond

func (jm *JobManager) refreshJobCtx(ctx context.Context) error {
	return jm.refreshJobCtxMinInterval(ctx, 100*time.Millisecond)
}

func (jm *JobManager) refreshJobCtxForce(ctx context.Context) error {
	return jm.refreshJobCtxMinInterval(ctx, 0)
}

func (jm *JobManager) refreshJobCtxMinInterval(ctx context.Context, minInterval time.Duration) error {
	jm.refreshMu.Lock()
	defer jm.refreshMu.Unlock()
	if minInterval > 0 && time.Since(jm.lastRefreshAttempt) < minInterval {
		return nil
	}
	jm.lastRefreshAttempt = time.Now()

	params := map[string]any{
		"rules":        []string{"segwit"},
		"capabilities": []string{"coinbasetxn", "workid", "coinbase/append"},
	}
	tpl, err := jm.fetchTemplateCtx(ctx, params, false)
	if err != nil {
		jm.recordJobError(err)
		return err
	}
	return jm.refreshFromTemplate(ctx, tpl)
}

func (jm *JobManager) fetchTemplateCtx(ctx context.Context, params map[string]any, useLongPoll bool) (GetBlockTemplateResult, error) {
	var tpl GetBlockTemplateResult
	var err error
	if useLongPoll {
		err = jm.rpc.callLongPollCtx(ctx, "getblocktemplate", []any{params}, &tpl)
	} else {
		err = jm.rpc.callCtx(ctx, "getblocktemplate", []any{params}, &tpl)
	}
	return tpl, err
}

func (jm *JobManager) refreshFromTemplate(ctx context.Context, tpl GetBlockTemplateResult) error {
	return jm.refreshFromTemplateWithReason(ctx, tpl, "", false)
}

func (jm *JobManager) refreshFromTemplateWithReason(ctx context.Context, tpl GetBlockTemplateResult, reason string, syncBroadcast bool) error {
	jm.applyMu.Lock()
	defer jm.applyMu.Unlock()

	needsNewJob, clean := jm.templateChanged(tpl)

	// If the template hasn't meaningfully changed, skip building and broadcasting a new job.
	// This avoids unnecessary job churn and duplicate JobIDs for the same work.
	if !needsNewJob {
		// Heartbeat: the node responded successfully, even if the template was unchanged.
		jm.recordJobSuccess(nil)
		jm.updateBlockTipFromTemplate(tpl)
		return nil
	}

	job, err := jm.buildJob(ctx, tpl)
	if err != nil {
		jm.recordJobError(err)
		return err
	}
	job.Clean = clean
	if reason != "" {
		job.NotifyReason = reason
	} else if clean {
		job.NotifyReason = "tip_update"
	} else {
		job.NotifyReason = "template_update"
	}

	jm.mu.Lock()
	jm.curJob = job
	jm.mu.Unlock()

	prevHeight := jm.blockTipHeight()

	jm.recordJobSuccess(job)
	jm.updateBlockTipFromTemplate(tpl)
	if tpl.Height > prevHeight {
		jm.refreshBlockHistoryFromRPC(ctx)
	}
	logger.Info("new job", "height", tpl.Height, "job_id", job.JobID, "bits", tpl.Bits, "txs", len(tpl.Transactions))
	if syncBroadcast {
		jm.broadcastJobSync(job)
	} else {
		jm.broadcastJob(job)
	}
	return nil
}

// refreshAfterAcceptedBlock forces a short poll loop until a template for the
// next height is available, then publishes it immediately with clean work.
func (jm *JobManager) refreshAfterAcceptedBlock(ctx context.Context, acceptedHeight int64) error {
	if jm == nil {
		return fmt.Errorf("job manager is nil")
	}
	if jm.rpc == nil {
		return fmt.Errorf("rpc unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		params := map[string]any{
			"rules":        []string{"segwit"},
			"capabilities": []string{"coinbasetxn", "workid", "coinbase/append"},
		}
		tpl, err := jm.fetchTemplateCtx(ctx, params, false)
		if err != nil {
			jm.recordJobError(err)
			if err := sleepContext(ctx, acceptedBlockRefreshPollInterval); err != nil {
				return err
			}
			continue
		}

		if tpl.Height <= acceptedHeight {
			if err := sleepContext(ctx, acceptedBlockRefreshPollInterval); err != nil {
				return err
			}
			continue
		}

		return jm.refreshFromTemplateWithReason(ctx, tpl, "accepted_block", true)
	}
}
