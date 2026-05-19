package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/btcsuite/btcd/btcec/v2"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const sv2NoiseProtocolName = "Noise_NX_secp256k1_ChaChaPoly_SHA256"

type noiseState struct {
	ck     [32]byte
	h      [32]byte
	k      [32]byte
	n      uint64
	hasKey bool
}

func noiseInitializeSymmetric(protocolName string) noiseState {
	var s noiseState
	nameBytes := []byte(protocolName)
	if len(nameBytes) <= 32 {
		copy(s.ck[:], nameBytes)
	} else {
		s.ck = sha256.Sum256(nameBytes)
	}
	s.h = s.ck
	return s
}

func (s *noiseState) mixHash(data []byte) {
	h := sha256.New()
	h.Write(s.h[:])
	h.Write(data)
	copy(s.h[:], h.Sum(nil))
}

func noiseHKDF(ck [32]byte, ikm []byte) (ck2 [32]byte, k [32]byte) {
	r := hkdf.New(sha256.New, ikm, ck[:], nil)
	var out [64]byte
	if _, err := io.ReadFull(r, out[:]); err != nil {
		panic(fmt.Sprintf("sv2 hkdf: %v", err))
	}
	copy(ck2[:], out[:32])
	copy(k[:], out[32:])
	return
}

func (s *noiseState) mixKey(ikm []byte) {
	s.ck, s.k = noiseHKDF(s.ck, ikm)
	s.n = 0
	s.hasKey = true
}

func noiseNonce(n uint64) [12]byte {
	var nonce [12]byte
	binary.LittleEndian.PutUint64(nonce[4:], n)
	return nonce
}

func (s *noiseState) encryptAndHash(plaintext []byte) ([]byte, error) {
	if !s.hasKey {
		s.mixHash(plaintext)
		return plaintext, nil
	}
	aead, err := chacha20poly1305.New(s.k[:])
	if err != nil {
		return nil, err
	}
	nonce := noiseNonce(s.n)
	ciphertext := aead.Seal(nil, nonce[:], plaintext, s.h[:])
	s.mixHash(ciphertext)
	s.n++
	return ciphertext, nil
}

func (s *noiseState) decryptAndHash(ciphertext []byte) ([]byte, error) {
	if !s.hasKey {
		s.mixHash(ciphertext)
		return ciphertext, nil
	}
	aead, err := chacha20poly1305.New(s.k[:])
	if err != nil {
		return nil, err
	}
	nonce := noiseNonce(s.n)
	plaintext, err := aead.Open(nil, nonce[:], ciphertext, s.h[:])
	if err != nil {
		return nil, fmt.Errorf("noise decrypt: %w", err)
	}
	s.mixHash(ciphertext)
	s.n++
	return plaintext, nil
}

func (s *noiseState) split() (k1, k2 [32]byte) {
	k1, k2 = noiseHKDF(s.ck, nil)
	return
}

func noiseECDH(priv *btcec.PrivateKey, pub *btcec.PublicKey) []byte {
	return btcec.GenerateSharedSecret(priv, pub)
}

// sv2NoiseHandshake performs the NX responder handshake and returns an encrypted transport conn.
func sv2NoiseHandshake(conn net.Conn, staticKey *sv2StaticKey) (net.Conn, error) {
	s := noiseInitializeSymmetric(sv2NoiseProtocolName)
	// Empty prologue
	s.mixHash([]byte{})

	// Step 1: Read 33-byte compressed ephemeral pubkey from initiator
	var ePubBytes [33]byte
	if _, err := io.ReadFull(conn, ePubBytes[:]); err != nil {
		return nil, fmt.Errorf("sv2 noise: read ephemeral pubkey: %w", err)
	}
	ePub, err := btcec.ParsePubKey(ePubBytes[:])
	if err != nil {
		return nil, fmt.Errorf("sv2 noise: parse ephemeral pubkey: %w", err)
	}
	s.mixHash(ePubBytes[:])

	// Step 2: Generate responder ephemeral key and send it
	re, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("sv2 noise: gen ephemeral key: %w", err)
	}
	rePub := re.PubKey().SerializeCompressed()
	if _, err := conn.Write(rePub); err != nil {
		return nil, fmt.Errorf("sv2 noise: send ephemeral pubkey: %w", err)
	}
	s.mixHash(rePub)

	// Step 3: ee = ECDH(re, ePub)
	ee := noiseECDH(re, ePub)
	s.mixKey(ee)

	// Step 4: Encrypt and send static pubkey
	encStatic, err := s.encryptAndHash(staticKey.pubKey.SerializeCompressed())
	if err != nil {
		return nil, fmt.Errorf("sv2 noise: encrypt static: %w", err)
	}
	if _, err := conn.Write(encStatic); err != nil {
		return nil, fmt.Errorf("sv2 noise: send static: %w", err)
	}

	// Step 5: es = ECDH(staticKey, ePub)
	es := noiseECDH(staticKey.privKey, ePub)
	s.mixKey(es)

	// Step 6: Encrypt empty payload
	encEmpty, err := s.encryptAndHash([]byte{})
	if err != nil {
		return nil, fmt.Errorf("sv2 noise: encrypt empty: %w", err)
	}
	if _, err := conn.Write(encEmpty); err != nil {
		return nil, fmt.Errorf("sv2 noise: send empty: %w", err)
	}

	// Step 7: Split into transport keys
	// k1 = initiator→responder (responder receives with k1)
	// k2 = responder→initiator (responder sends with k2)
	k1, k2 := s.split()

	return &sv2EncryptedConn{
		Conn:    conn,
		sendKey: k2,
		recvKey: k1,
	}, nil
}

// sv2EncryptedConn wraps a net.Conn with ChaCha20-Poly1305 framed encryption.
// Each write encrypts the plaintext and prepends a 2-byte LE ciphertext length.
// Each read reads the 2-byte length, reads that many bytes, decrypts.
type sv2EncryptedConn struct {
	net.Conn
	sendKey [32]byte
	recvKey [32]byte
	sendN   uint64
	recvN   uint64
	sendMu  sync.Mutex
	recvMu  sync.Mutex
	readBuf []byte
	readPos int
}

func (c *sv2EncryptedConn) Write(p []byte) (int, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	aead, err := chacha20poly1305.New(c.sendKey[:])
	if err != nil {
		return 0, err
	}
	nonce := noiseNonce(c.sendN)
	c.sendN++
	ciphertext := aead.Seal(nil, nonce[:], p, nil)

	// Prepend 2-byte LE length
	l := uint16(len(ciphertext))
	hdr := [2]byte{byte(l), byte(l >> 8)}
	if _, err := c.Conn.Write(hdr[:]); err != nil {
		return 0, err
	}
	if _, err := c.Conn.Write(ciphertext); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *sv2EncryptedConn) Read(p []byte) (int, error) {
	c.recvMu.Lock()
	defer c.recvMu.Unlock()

	// Return buffered decrypted data if available
	if c.readPos < len(c.readBuf) {
		n := copy(p, c.readBuf[c.readPos:])
		c.readPos += n
		return n, nil
	}

	// Read 2-byte LE length header
	var hdr [2]byte
	if _, err := io.ReadFull(c.Conn, hdr[:]); err != nil {
		return 0, err
	}
	cLen := int(binary.LittleEndian.Uint16(hdr[:]))

	ciphertext := make([]byte, cLen)
	if _, err := io.ReadFull(c.Conn, ciphertext); err != nil {
		return 0, err
	}

	aead, err := chacha20poly1305.New(c.recvKey[:])
	if err != nil {
		return 0, err
	}
	nonce := noiseNonce(c.recvN)
	c.recvN++
	plaintext, err := aead.Open(nil, nonce[:], ciphertext, nil)
	if err != nil {
		return 0, fmt.Errorf("sv2 transport decrypt: %w", err)
	}

	c.readBuf = plaintext
	c.readPos = 0
	n := copy(p, plaintext)
	c.readPos = n
	return n, nil
}
