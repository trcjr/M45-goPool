package main

import (
	"io"
	"net"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"golang.org/x/crypto/chacha20poly1305"
)

// testNoiseInitiator implements the NX initiator side for testing.
func testNoiseInitiator(conn net.Conn) (net.Conn, error) {
	s := noiseInitializeSymmetric(sv2NoiseProtocolName)
	s.mixHash([]byte{})

	// Generate ephemeral key
	ePriv, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, err
	}
	ePubBytes := ePriv.PubKey().SerializeCompressed()
	s.mixHash(ePubBytes)

	// Send ephemeral pubkey (33 bytes)
	if _, err := conn.Write(ePubBytes); err != nil {
		return nil, err
	}

	// Read responder ephemeral pubkey (33 bytes)
	var rePubBytes [33]byte
	if _, err := io.ReadFull(conn, rePubBytes[:]); err != nil {
		return nil, err
	}
	s.mixHash(rePubBytes[:])
	rePub, err := btcec.ParsePubKey(rePubBytes[:])
	if err != nil {
		return nil, err
	}

	// ee = ECDH(ePriv, rePub)
	ee := noiseECDH(ePriv, rePub)
	s.mixKey(ee)

	// Read encrypted static pubkey (33 + 16 bytes AEAD tag)
	encStatic := make([]byte, 33+chacha20poly1305.Overhead)
	if _, err := io.ReadFull(conn, encStatic); err != nil {
		return nil, err
	}
	staticPubBytes, err := s.decryptAndHash(encStatic)
	if err != nil {
		return nil, err
	}
	staticPub, err := btcec.ParsePubKey(staticPubBytes)
	if err != nil {
		return nil, err
	}

	// es = ECDH(ePriv, staticPub)
	es := noiseECDH(ePriv, staticPub)
	s.mixKey(es)

	// Read encrypted empty payload (0 + 16 bytes AEAD tag)
	encEmpty := make([]byte, chacha20poly1305.Overhead)
	if _, err := io.ReadFull(conn, encEmpty); err != nil {
		return nil, err
	}
	if _, err := s.decryptAndHash(encEmpty); err != nil {
		return nil, err
	}

	// Split keys: initiator sends with k1, receives with k2
	k1, k2 := s.split()
	return &sv2EncryptedConn{
		Conn:    conn,
		sendKey: k1,
		recvKey: k2,
	}, nil
}

func TestSV2NoiseHandshake(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	staticKey := &sv2StaticKey{privKey: privKey, pubKey: privKey.PubKey()}

	errCh := make(chan error, 2)
	type result struct {
		conn net.Conn
		err  error
	}
	serverCh := make(chan result, 1)
	clientCh := make(chan result, 1)

	go func() {
		conn, err := sv2NoiseHandshake(serverConn, staticKey)
		serverCh <- result{conn, err}
	}()
	go func() {
		conn, err := testNoiseInitiator(clientConn)
		clientCh <- result{conn, err}
	}()

	sr := <-serverCh
	cr := <-clientCh
	if sr.err != nil {
		t.Fatal("server handshake error:", sr.err)
	}
	if cr.err != nil {
		t.Fatal("client handshake error:", cr.err)
	}

	serverEnc := sr.conn
	clientEnc := cr.conn

	// Test encrypted communication: server → client
	msg := []byte("hello sv2")
	go func() {
		_, err := serverEnc.Write(msg)
		errCh <- err
	}()
	go func() {
		buf := make([]byte, len(msg))
		_, err := io.ReadFull(clientEnc, buf)
		if err == nil && string(buf) != string(msg) {
			err = io.ErrUnexpectedEOF
		}
		errCh <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal("transport error:", err)
		}
	}
}
