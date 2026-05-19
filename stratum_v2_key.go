package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil/base58"
)

type sv2StaticKey struct {
	privKey *btcec.PrivateKey
	pubKey  *btcec.PublicKey
}

func loadOrGenerateSV2Key(path string) (*sv2StaticKey, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("sv2 key dir: %w", err)
	}
	data, err := os.ReadFile(path)
	if err == nil && len(data) == 32 {
		privKey, pubKey := btcec.PrivKeyFromBytes(data)
		return &sv2StaticKey{privKey: privKey, pubKey: pubKey}, nil
	}
	// Generate new key
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("sv2 key gen: %w", err)
	}
	keyBytes := privKey.Serialize() // 32-byte big-endian scalar
	if err := os.WriteFile(path, keyBytes, 0600); err != nil {
		return nil, fmt.Errorf("sv2 key save: %w", err)
	}
	return &sv2StaticKey{privKey: privKey, pubKey: privKey.PubKey()}, nil
}

func (k *sv2StaticKey) pubHex() string {
	return hex.EncodeToString(k.pubKey.SerializeCompressed())
}

// authKeyBase58Check encodes the 32-byte x-only secp256k1 public key in the
// format required by SV2 spec §4.7 (URL Scheme and Pool Authority Key):
//
//	[0x01, 0x00]  LE u16 version prefix (version = 1)
//	<32 bytes>    x-only public key (BIP-340 convention)
//
// The resulting 34-byte payload is base58check encoded. Miners embed this
// value in their upstream URL: stratum2+tcp://pool.example.com:3333/<key>
func (k *sv2StaticKey) authKeyBase58Check() string {
	// X coordinate as 32-byte big-endian (BIP-340 x-only key).
	xBytes := k.pubKey.X().Bytes()
	xOnly := make([]byte, 32)
	copy(xOnly[32-len(xBytes):], xBytes)

	// Prepend LE u16 version = 1 → [0x01, 0x00].
	payload := make([]byte, 34)
	payload[0] = 0x01
	payload[1] = 0x00
	copy(payload[2:], xOnly)

	// Double-SHA256 checksum (standard base58check).
	h1 := sha256.Sum256(payload)
	h2 := sha256.Sum256(h1[:])

	full := append(payload, h2[:4]...)
	return base58.Encode(full)
}
