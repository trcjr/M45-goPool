package main

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// TestSV1SV2HashingCompatibility verifies that, given equivalent mining inputs,
// SV1 and SV2 block construction paths produce identical block bytes and header
// hash results.
func TestSV1SV2HashingCompatibility(t *testing.T) {
	workerName, _, workerScript := generateTestWorker(t)
	job := benchmarkSubmitJobForTest(t)

	// Ensure both paths use the same payout behavior.
	cfg := Config{PoolFeePercent: 0}
	extranonce1 := []byte{0x01, 0x02, 0x03, 0x04}
	extranonce2 := []byte{0x0a, 0x0b, 0x0c, 0x0d}
	nTimeHex := "6553f100"
	nonceHex := "1a2b3c4d"
	version := int32(0x20000000)

	sv1BlockHex, sv1HeaderHash, _, _, err := buildBlockWithScriptTime(
		job,
		extranonce1,
		extranonce2,
		nTimeHex,
		nonceHex,
		version,
		workerScript,
		job.ScriptTime,
	)
	if err != nil {
		t.Fatalf("sv1 buildBlockWithScriptTime: %v", err)
	}

	c := &sv2Conn{
		cfg:        cfg,
		workerName: workerName,
		extranonce1: append([]byte(nil), extranonce1...),
	}
	coinb1, coinb2, err := c.buildPayoutCoinbaseParts(job, job.Extranonce2Size, job.TemplateExtraNonce2Size)
	if err != nil {
		t.Fatalf("sv2 buildPayoutCoinbaseParts: %v", err)
	}

	coinb1Bytes, err := hex.DecodeString(coinb1)
	if err != nil {
		t.Fatalf("decode coinb1: %v", err)
	}
	coinb2Bytes, err := hex.DecodeString(coinb2)
	if err != nil {
		t.Fatalf("decode coinb2: %v", err)
	}
	cb := make([]byte, 0, len(coinb1Bytes)+len(extranonce1)+len(extranonce2)+len(coinb2Bytes))
	cb = append(cb, coinb1Bytes...)
	cb = append(cb, extranonce1...)
	cb = append(cb, extranonce2...)
	cb = append(cb, coinb2Bytes...)

	coinbaseTxID := doubleSHA256(cb)
	merkleRootBE := computeMerkleRootFromBranches(coinbaseTxID, job.MerkleBranches)
	if merkleRootBE == nil {
		t.Fatal("sv2 merkle root is nil")
	}

	hdr, err := buildBlockHeaderFromHex(version, job.Template.Previous, merkleRootBE, nTimeHex, job.Template.Bits, nonceHex)
	if err != nil {
		t.Fatalf("sv2 buildBlockHeaderFromHex: %v", err)
	}

	var blockBuf bytes.Buffer
	blockBuf.Write(hdr)
	writeVarInt(&blockBuf, uint64(1+len(job.Transactions)))
	blockBuf.Write(cb)
	for _, tx := range job.Transactions {
		raw, err := hex.DecodeString(tx.Data)
		if err != nil {
			t.Fatalf("decode tx data: %v", err)
		}
		blockBuf.Write(raw)
	}
	sv2BlockHex := hex.EncodeToString(blockBuf.Bytes())

	if sv1BlockHex != sv2BlockHex {
		t.Fatalf("sv1/sv2 block hex mismatch\nsv1=%s\nsv2=%s", sv1BlockHex, sv2BlockHex)
	}

	sv2HeaderHash := doubleSHA256(hdr)
	reverseBytes(sv2HeaderHash)
	if !bytes.Equal(sv1HeaderHash, sv2HeaderHash) {
		t.Fatalf("sv1/sv2 header hash mismatch\nsv1=%x\nsv2=%x", sv1HeaderHash, sv2HeaderHash)
	}
}
