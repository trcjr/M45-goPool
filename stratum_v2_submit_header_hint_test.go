package main

import (
	"encoding/hex"
	"testing"
)

func TestSV2SubmitExtendedDecodeWithTrailingHeaderHint(t *testing.T) {
	msg := sv2SubmitSharesExtended{
		ChannelID:      7,
		SequenceNumber: 11,
		JobID:          23,
		Nonce:          0x01020304,
		NTime:          0x11121314,
		Version:        0x20000000,
		Extranonce:     []byte{0xaa, 0xbb, 0xcc, 0xdd},
	}
	payload := msg.encode()
	headerHint := make([]byte, 80)
	for i := range headerHint {
		headerHint[i] = byte(i)
	}
	payload = append(payload, headerHint...)

	var out sv2SubmitSharesExtended
	trailing, err := out.decodeWithTrailing(payload)
	if err != nil {
		t.Fatalf("decodeWithTrailing: %v", err)
	}
	if out.ChannelID != msg.ChannelID || out.SequenceNumber != msg.SequenceNumber || out.JobID != msg.JobID {
		t.Fatalf("decoded fields mismatch")
	}
	if len(trailing) != 80 {
		t.Fatalf("trailing len = %d, want 80", len(trailing))
	}
	if _, source, ok := sv2ParseSubmitHeaderHint(trailing); !ok || source != "raw80" {
		t.Fatalf("expected raw80 header hint, got ok=%v source=%q", ok, source)
	}
}

func TestSV2SubmitStandardDecodeWithTrailingHeaderHint(t *testing.T) {
	msg := sv2SubmitSharesStandard{
		ChannelID:      3,
		SequenceNumber: 9,
		JobID:          17,
		Nonce:          0x01020304,
		NTime:          0x11121314,
		Version:        0x20000000,
	}
	payload := msg.encode()
	headerHint := make([]byte, 80)
	for i := range headerHint {
		headerHint[i] = 0xaa - byte(i)
	}
	payload = append(payload, headerHint...)

	var out sv2SubmitSharesStandard
	trailing, err := out.decodeWithTrailing(payload)
	if err != nil {
		t.Fatalf("decodeWithTrailing: %v", err)
	}
	if out.ChannelID != msg.ChannelID || out.SequenceNumber != msg.SequenceNumber || out.JobID != msg.JobID {
		t.Fatalf("decoded fields mismatch")
	}
	if len(trailing) != 80 {
		t.Fatalf("trailing len = %d, want 80", len(trailing))
	}
	if _, source, ok := sv2ParseSubmitHeaderHint(trailing); !ok || source != "raw80" {
		t.Fatalf("expected raw80 header hint, got ok=%v source=%q", ok, source)
	}
}

func TestSV2SubmitHeaderHintHex160(t *testing.T) {
	h := make([]byte, 80)
	for i := range h {
		h[i] = 0xff - byte(i)
	}
	hexHint := []byte(hex.EncodeToString(h))
	got, source, ok := sv2ParseSubmitHeaderHint(hexHint)
	if !ok {
		t.Fatal("expected hex160 hint parse to succeed")
	}
	if source != "hex160" {
		t.Fatalf("source=%q want hex160", source)
	}
	if len(got) != 80 {
		t.Fatalf("header len=%d want 80", len(got))
	}
}

func TestSV2MerkleOnlyHeaderDiffClassifier(t *testing.T) {
	if !sv2DiffIsMerkleRootOnly([]int{36, 40, 67}) {
		t.Fatal("expected merkle-only offset set to be classified true")
	}
	if sv2DiffIsMerkleRootOnly([]int{35, 36}) {
		t.Fatal("offset outside merkle field should classify false")
	}
	if sv2DiffIsMerkleRootOnly(nil) {
		t.Fatal("empty offsets should classify false")
	}
}
