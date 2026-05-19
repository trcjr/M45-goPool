package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (s *StatusServer) SetJobManager(jm *JobManager) {
	if s == nil {
		return
	}
	s.jobMgr = jm
	if jm != nil {
		// Set up callback to invalidate status cache when new blocks arrive.
		jm.onNewBlock = s.invalidateStatusCache
	}
}

func (s *StatusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/favicon.png":
		if s != nil && s.staticFiles != nil && s.staticFiles.ServePath(w, r, "favicon.png") {
			return
		}
		http.NotFound(w, r)

	case r.URL.Path == "/" || r.URL.Path == "":
		if err := s.serveOverviewOrNodeDown(w); err != nil {
			logger.Error("status template error", "error", err)
			s.renderErrorPage(w, r, http.StatusInternalServerError,
				"Status page error",
				"We couldn't render the pool status page.",
				"Template error while rendering the main status view.")
		}
	default:
		s.renderErrorPage(w, r, http.StatusNotFound,
			"Page not found",
			"The page you requested could not be found.",
			"Check the URL or use the navigation links above.")
	}
}

type nodeDownTemplateData struct {
	StatusData
	Title   string
	Message string
	Detail  string
}

func (s *StatusServer) serveOverviewOrNodeDown(w http.ResponseWriter) error {
	start := time.Now()
	data := s.baseTemplateData(start)

	templateName := "overview"
	view := any(data)

	h := stratumHealthStatus(s.jobMgr, time.Now())
	job := (*Job)(nil)
	fs := JobFeedStatus{}
	if s.jobMgr != nil {
		job = s.jobMgr.CurrentJob()
		fs = s.jobMgr.FeedStatus()
	}

	if !s.start.IsZero() && time.Since(s.start) < stratumStartupGrace {
		h = stratumHealth{Healthy: true}
	}

	if !h.Healthy {
		title := "Node connection unavailable"
		message := "This pool does not currently have a connection to a node."
		switch h.Reason {
		case "node syncing/indexing":
			title = "Node indexing/syncing"
			message = "The pool is temporarily paused because the node is syncing/indexing."
		case "node/job feed error":
			title = "Node connection unavailable"
			message = "This pool does not currently have a connection to a node."
		case "no job template available", "no successful job refresh yet":
			title = "Node work unavailable"
			message = "The pool does not have usable work available from the node right now."
		}

		detailParts := make([]string, 0, 4)
		if job != nil && !job.CreatedAt.IsZero() {
			age := time.Since(job.CreatedAt)
			detailParts = append(detailParts, "last job: "+humanShortDuration(age)+" ago")
		} else {
			detailParts = append(detailParts, "last job: (none)")
		}
		if !fs.LastSuccess.IsZero() {
			detailParts = append(detailParts, "last success: "+humanShortDuration(time.Since(fs.LastSuccess))+" ago")
		}
		if fs.LastError != nil {
			detailParts = append(detailParts, "last error: "+strings.TrimSpace(fs.LastError.Error()))
		}
		if strings.TrimSpace(h.Detail) != "" {
			// Avoid repeating the same error string twice when h.Detail is derived
			// from FeedStatus.LastError.
			if fs.LastError == nil || strings.TrimSpace(fs.LastError.Error()) != strings.TrimSpace(h.Detail) {
				detailParts = append(detailParts, h.Detail)
			}
		}

		// If we can still reach the node for basic info, surface sync/indexing
		// state (IBD, headers/blocks) to help operators diagnose common "indexing"
		// / "verifying blocks" scenarios.
		info := s.ensureNodeInfo()
		if !info.fetchedAt.IsZero() && (info.ibd || (info.headers > 0 && info.blocks > 0 && info.blocks < info.headers)) {
			detailParts = append(detailParts, fmt.Sprintf("node syncing: ibd=%v blocks=%d headers=%d", info.ibd, info.blocks, info.headers))
		}

		templateName = "node_down"
		view = nodeDownTemplateData{
			StatusData: data,
			Title:      title,
			Message:    message,
			Detail:     strings.Join(detailParts, " — "),
		}
	}

	var buf bytes.Buffer
	if err := s.executeTemplate(&buf, templateName, view); err != nil {
		return err
	}
	setShortHTMLCacheHeaders(w, false)
	_, err := w.Write(buf.Bytes())
	if err != nil {
		logResponseWriteDebug("write overview html response", err, "template", templateName)
		return nil
	}
	return nil
}

// handleWellKnownStratum serves /.well-known/stratum per the SV2 specification.
// It returns a JSON object containing the pool's NOISE static public key so
// miners can verify the pool's identity during the NOISE NX handshake without
// out-of-band key distribution.
//
// The "authority_key" field is the Pool Authority Public Key encoded per SV2
// spec §4.7: base58check([0x01, 0x00] || 32-byte x-only secp256k1 pubkey).
// Miners embed it in the upstream URL:
//
//	stratum2+tcp://pool.example.com:3333/<authority_key>
func (s *StatusServer) handleWellKnownStratum(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s == nil || s.sv2NoiseKey == nil {
		http.Error(w, "stratum v2 not configured", http.StatusNotFound)
		return
	}
	pubHex := s.sv2NoiseKey.pubHex()
	authorityKey := s.sv2NoiseKey.authKeyBase58Check()
	payload := fmt.Sprintf(
		"{\"noise_pubkey\":%q,\"authority_key\":%q,\"protocol\":\"Noise_NX_secp256k1_ChaChaPoly_SHA256\",\"version\":2}\n",
		pubHex, authorityKey,
	)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(payload))
	}
}
