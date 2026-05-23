package main

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestSV2StandardCoinbaseReconstructionParsesFully(t *testing.T) {
	extranonce1 := []byte{0x01, 0x02, 0x03, 0x04}
	extranonce2Size := 4
	templateEx2Size := 8

	coinb1, coinb2, err := buildCoinbaseParts(
		101,
		extranonce1,
		extranonce2Size,
		templateEx2Size,
		[]byte{0x51},
		50*1e8,
		"",
		"",
		"sv2-standard-coinbase-parse",
		0,
	)
	if err != nil {
		t.Fatalf("buildCoinbaseParts: %v", err)
	}

	coinb1Raw, err := hex.DecodeString(coinb1)
	if err != nil {
		t.Fatalf("decode coinb1: %v", err)
	}
	coinb2Raw, err := hex.DecodeString(coinb2)
	if err != nil {
		t.Fatalf("decode coinb2: %v", err)
	}

	fixedExtranonce := make([]byte, 0, len(extranonce1)+extranonce2Size)
	fixedExtranonce = append(fixedExtranonce, extranonce1...)
	fixedExtranonce = append(fixedExtranonce, make([]byte, extranonce2Size)...)
	fullCoinbase := make([]byte, 0, len(coinb1Raw)+len(fixedExtranonce)+len(coinb2Raw))
	fullCoinbase = append(fullCoinbase, coinb1Raw...)
	fullCoinbase = append(fullCoinbase, fixedExtranonce...)
	fullCoinbase = append(fullCoinbase, coinb2Raw...)

	_, _, consumed, err := parseSerializedTxForSubmitBlock(fullCoinbase)
	if err != nil {
		t.Fatalf("parseSerializedTxForSubmitBlock(full): %v", err)
	}
	if consumed != len(fullCoinbase) {
		t.Fatalf("consumed bytes = %d, want %d", consumed, len(fullCoinbase))
	}
	if _, err := txidFromSerializedTx(fullCoinbase); err != nil {
		t.Fatalf("txidFromSerializedTx(full): %v", err)
	}

	// Regression check for the failing shape: inserting extranonce1 again causes
	// a serialized tx with trailing bytes under parser rules.
	legacyInsert := make([]byte, 0, len(extranonce1)+templateEx2Size)
	legacyInsert = append(legacyInsert, extranonce1...)
	legacyInsert = append(legacyInsert, make([]byte, templateEx2Size)...)
	legacyCoinbase := make([]byte, 0, len(coinb1Raw)+len(legacyInsert)+len(coinb2Raw))
	legacyCoinbase = append(legacyCoinbase, coinb1Raw...)
	legacyCoinbase = append(legacyCoinbase, legacyInsert...)
	legacyCoinbase = append(legacyCoinbase, coinb2Raw...)

	_, _, legacyConsumed, legacyParseErr := parseSerializedTxForSubmitBlock(legacyCoinbase)
	if legacyParseErr != nil {
		t.Fatalf("legacy parse unexpectedly failed before trailing-byte check: %v", legacyParseErr)
	}
	if legacyConsumed >= len(legacyCoinbase) {
		t.Fatalf("legacy consumed=%d total=%d, expected trailing bytes", legacyConsumed, len(legacyCoinbase))
	}
	if _, err := txidFromSerializedTx(legacyCoinbase); err == nil || !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("expected trailing-bytes error for legacy shape, got: %v", err)
	}
}

func TestSV2StandardCoinbaseZeroBranchMerkleAndBlockSerialization(t *testing.T) {
	extranonce1 := []byte{0x0a, 0x0b, 0x0c, 0x0d}
	extranonce2Size := 4
	templateEx2Size := 8

	coinb1, coinb2, err := buildCoinbaseParts(
		102,
		extranonce1,
		extranonce2Size,
		templateEx2Size,
		[]byte{0x51},
		50*1e8,
		"",
		"",
		"sv2-standard-merkle",
		0,
	)
	if err != nil {
		t.Fatalf("buildCoinbaseParts: %v", err)
	}
	coinb1Raw, err := hex.DecodeString(coinb1)
	if err != nil {
		t.Fatalf("decode coinb1: %v", err)
	}
	coinb2Raw, err := hex.DecodeString(coinb2)
	if err != nil {
		t.Fatalf("decode coinb2: %v", err)
	}

	fixedExtranonce := make([]byte, 0, len(extranonce1)+extranonce2Size)
	fixedExtranonce = append(fixedExtranonce, extranonce1...)
	fixedExtranonce = append(fixedExtranonce, make([]byte, extranonce2Size)...)
	coinbaseTx := make([]byte, 0, len(coinb1Raw)+len(fixedExtranonce)+len(coinb2Raw))
	coinbaseTx = append(coinbaseTx, coinb1Raw...)
	coinbaseTx = append(coinbaseTx, fixedExtranonce...)
	coinbaseTx = append(coinbaseTx, coinb2Raw...)

	coinbaseTxID, err := txidFromSerializedTx(coinbaseTx)
	if err != nil {
		t.Fatalf("txidFromSerializedTx: %v", err)
	}
	trace, ok := buildSV2MerkleTrace(coinbaseTxID[:], nil)
	if !ok {
		t.Fatal("buildSV2MerkleTrace failed")
	}
	if trace.MerkleRootWireLE != hex.EncodeToString(coinbaseTxID[:]) {
		t.Fatalf("zero-branch merkle root mismatch: got %s want %s", trace.MerkleRootWireLE, hex.EncodeToString(coinbaseTxID[:]))
	}

	header80, err := buildBlockHeaderFromHex(
		0x20000000,
		strings.Repeat("0", 64),
		coinbaseTxID[:],
		"6553f100",
		"1d00ffff",
		"00000001",
	)
	if err != nil {
		t.Fatalf("buildBlockHeaderFromHex: %v", err)
	}

	block := make([]byte, 0, 80+1+len(coinbaseTx))
	block = append(block, header80...)
	block = append(block, 0x01) // CompactSize tx_count=1
	block = append(block, coinbaseTx...)

	if len(block) != 81+len(coinbaseTx) {
		t.Fatalf("block len = %d, want %d", len(block), 81+len(coinbaseTx))
	}
	if block[80] != 0x01 {
		t.Fatalf("tx count varint byte = 0x%02x, want 0x01", block[80])
	}

	txRaw, _, consumed, err := parseSerializedTxForSubmitBlock(block[81:])
	if err != nil {
		t.Fatalf("parse tx from block: %v", err)
	}
	if consumed != len(coinbaseTx) {
		t.Fatalf("coinbase consumed bytes = %d, want %d", consumed, len(coinbaseTx))
	}
	if hex.EncodeToString(txRaw) != hex.EncodeToString(coinbaseTx) {
		t.Fatal("coinbase tx bytes in block payload mismatch")
	}
}
