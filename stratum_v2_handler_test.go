package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestSV2BuildSetNewPrevHashUsesCanonicalNBits(t *testing.T) {
	job := benchmarkSubmitJobForTest(t)
	// 1d00ffff is the canonical compact for diff1. If byte-swapped, receivers
	// parse 0xffff001d and compute nonsense network difficulty.
	job.Template.Bits = "1d00ffff"
	if err := decodeHex8To4(&job.bitsBytes, job.Template.Bits); err != nil {
		t.Fatalf("decode bits: %v", err)
	}

	msg := sv2BuildSetNewPrevHash(job, 7, 9)
	if msg.NBits != 0x1d00ffff {
		t.Fatalf("NBits=%08x want 1d00ffff", msg.NBits)
	}

	payload := msg.encode()
	gotWire := binary.LittleEndian.Uint32(payload[44:48])
	if gotWire != 0x1d00ffff {
		t.Fatalf("wire NBits=%08x want 1d00ffff", gotWire)
	}
}

// TestSV2BuildSetNewPrevHashSendsPrevHashLE verifies that the prevhash in
// SetNewPrevHash is sent in internal/little-endian byte order.  Miners
// (e.g. BitAxe firmware) place the received bytes directly at offset 4-35 of
// the Bitcoin block header, which is the LE/internal format.  The pool
// receives prevhash from bitcoind in display/big-endian format; we must
// reverse it before sending.
func TestSV2BuildSetNewPrevHashSendsPrevHashLE(t *testing.T) {
	job := benchmarkSubmitJobForTest(t)

	// Use a non-trivial prevhash so byte-order errors are obvious.
	displayHex := "00000000000000000001604cb78883f1630757dfb9ecc0b8c4e3ce1a63d4a9a7"
	job.Template.Previous = displayHex
	if err := decodeHexToFixedBytes(job.prevHashBytes[:], displayHex); err != nil {
		t.Fatalf("decode prevhash: %v", err)
	}

	msg := sv2BuildSetNewPrevHash(job, 1, 2)

	// The PrevHash field on the message struct must be LE (reversed from display).
	displayBytes, _ := hex.DecodeString(displayHex)
	var wantLE [32]byte
	for i := 0; i < 32; i++ {
		wantLE[i] = displayBytes[31-i]
	}
	if msg.PrevHash != wantLE {
		t.Fatalf("PrevHash wrong byte order:\n got  %x\n want %x", msg.PrevHash, wantLE)
	}

	// The wire encoding (sv2AppendU256) writes the 32 bytes as-is; verify the
	// payload carries LE bytes at the prevhash offset (bytes 8-39 of the payload:
	// 4 channelID + 4 jobID = 8 bytes before PrevHash).
	payload := msg.encode()
	if !bytes.Equal(payload[8:40], wantLE[:]) {
		t.Fatalf("wire PrevHash wrong:\n got  %x\n want %x", payload[8:40], wantLE)
	}

	// Double-check: wire bytes must NOT equal the display/BE bytes.
	if bytes.Equal(payload[8:40], displayBytes) {
		t.Fatal("wire PrevHash is in big-endian display order; should be little-endian")
	}
}
