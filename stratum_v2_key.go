package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/btcec/v2"
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
