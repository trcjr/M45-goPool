package main

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestSubmitBlockRPCPreflightHeaderVarIntCoinbaseOnly(t *testing.T) {
	submitBlockCoinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0100ffffffff0100000000000000000000000000"
	coinbaseRaw, err := hex.DecodeString(submitBlockCoinbaseHex)
	if err != nil {
		t.Fatalf("decode coinbase: %v", err)
	}
	coinbaseTxID := doubleSHA256Array(coinbaseRaw)

	header80 := make([]byte, 80)
	binary.LittleEndian.PutUint32(header80[0:4], 0x20000000)
	copy(header80[36:68], coinbaseTxID[:])
	binary.LittleEndian.PutUint32(header80[68:72], 1700000000)
	binary.LittleEndian.PutUint32(header80[72:76], 0x1d00ffff)
	binary.LittleEndian.PutUint32(header80[76:80], 42)

	submitBlockHeader80Hex := hex.EncodeToString(header80)
	blockHex := submitBlockHeader80Hex + "01" + submitBlockCoinbaseHex

	preflight, err := preflightSubmitBlockRPC(blockHex)
	if err != nil {
		t.Fatalf("preflightSubmitBlockRPC: %v", err)
	}
	if preflight.rpcHeader80Hex != submitBlockHeader80Hex {
		t.Fatalf("rpcHeader80Hex mismatch: got %s want %s", preflight.rpcHeader80Hex, submitBlockHeader80Hex)
	}
	if preflight.rpcTxCountVarintHex != "01" {
		t.Fatalf("rpcTxCountVarintHex = %s, want 01", preflight.rpcTxCountVarintHex)
	}
	if preflight.rpcFirstTxHex != submitBlockCoinbaseHex {
		t.Fatalf("rpcFirstTxHex mismatch")
	}
	if preflight.parsedTxCount != 1 {
		t.Fatalf("parsedTxCount = %d, want 1", preflight.parsedTxCount)
	}
	if !preflight.merkleMatchesHeader {
		t.Fatalf("merkleMatchesHeader = false, header=%s recomputed=%s", preflight.headerMerkleWireLE, preflight.parsedMerkleWireLE)
	}
	if preflight.parsedMerkleWireLE != preflight.headerMerkleWireLE {
		t.Fatalf("parsed merkle %s != header merkle %s", preflight.parsedMerkleWireLE, preflight.headerMerkleWireLE)
	}
	expectedBlockHex := submitBlockHeader80Hex + "01" + submitBlockCoinbaseHex
	if preflight.rpcHex != expectedBlockHex {
		t.Fatalf("rpcHex mismatch")
	}
}
