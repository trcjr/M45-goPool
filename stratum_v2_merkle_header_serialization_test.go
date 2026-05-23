package main

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestSV2HeaderSerializationUsesMerkleWireLE(t *testing.T) {
	cases := []struct {
		name         string
		merkleWireLE string
		merkleBE     string
	}{
		{
			name:         "trace35",
			merkleWireLE: "527f043bab135fb7dcf0b2301685afb54d85c5d2a9d9d497b06e791c31871125",
			merkleBE:     "251187311c796eb097d4d9a9d2c5854db5af851630b2f0dcb75f13ab3b047f52",
		},
		{
			name:         "trace37",
			merkleWireLE: "2eb86aacbbaccc04aef1876d06f6bad316eacae89b587a4a040532f188adccd2",
			merkleBE:     "d2ccad88f13205044a7a589be8caea16d3baf6066d87f1ae04ccacbbac6ab82e",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			coinbaseTxID, err := hex.DecodeString(tc.merkleWireLE)
			if err != nil {
				t.Fatalf("decode coinbase txid: %v", err)
			}

			trace, ok := buildSV2MerkleTrace(coinbaseTxID, nil)
			if !ok {
				t.Fatal("buildSV2MerkleTrace returned !ok")
			}
			if trace.MerkleRootWireLE != tc.merkleWireLE {
				t.Fatalf("wire LE mismatch: got %s want %s", trace.MerkleRootWireLE, tc.merkleWireLE)
			}
			if trace.MerkleRootDisplayBE != tc.merkleBE {
				t.Fatalf("display BE mismatch: got %s want %s", trace.MerkleRootDisplayBE, tc.merkleBE)
			}

			merkleWire, err := hex.DecodeString(trace.MerkleRootWireLE)
			if err != nil {
				t.Fatalf("decode merkle wire LE: %v", err)
			}

			header, err := buildBlockHeaderFromHex(
				0x20000000,
				strings.Repeat("00", 31)+"01",
				merkleWire,
				"665f60aa",
				"207fffff",
				"01020304",
			)
			if err != nil {
				t.Fatalf("buildBlockHeaderFromHex: %v", err)
			}
			if len(header) != 80 {
				t.Fatalf("header length = %d, want 80", len(header))
			}

			merkleInHeaderHex := hex.EncodeToString(header[36:68])
			if merkleInHeaderHex != tc.merkleWireLE {
				t.Fatalf("serialized header merkle mismatch: got %s want %s", merkleInHeaderHex, tc.merkleWireLE)
			}
			if merkleInHeaderHex == tc.merkleBE {
				t.Fatalf("serialized header merkle used display BE unexpectedly: %s", merkleInHeaderHex)
			}
		})
	}
}
