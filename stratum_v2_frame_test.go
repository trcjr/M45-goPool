package main

import (
	"bytes"
	"io"
	"testing"
)

func TestSV2FrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := sv2WriteFrame(&buf, 0x1b, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	mt, payload, err := sv2ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if mt != 0x1b {
		t.Errorf("got msgType %x, want 0x1b", mt)
	}
	if !bytes.Equal(payload, []byte{1, 2, 3, 4}) {
		t.Errorf("got payload %v, want [1 2 3 4]", payload)
	}
}

func TestSV2Primitives(t *testing.T) {
	var b []byte
	b = sv2AppendU8(b, 0xAB)
	b = sv2AppendU16(b, 0x1234)
	b = sv2AppendU32(b, 0xDEADBEEF)
	b = sv2AppendU64(b, 0xCAFEBABE01020304)
	b = sv2AppendBool(b, true)
	b = sv2AppendStr(b, "hello")
	b = sv2AppendBytes(b, []byte{0xDE, 0xAD})

	off := 0
	if v, err := sv2ReadU8(b, &off); err != nil || v != 0xAB {
		t.Errorf("U8: got %v %v", v, err)
	}
	if v, err := sv2ReadU16(b, &off); err != nil || v != 0x1234 {
		t.Errorf("U16: got %v %v", v, err)
	}
	if v, err := sv2ReadU32(b, &off); err != nil || v != 0xDEADBEEF {
		t.Errorf("U32: got %v %v", v, err)
	}
	if v, err := sv2ReadU64(b, &off); err != nil || v != 0xCAFEBABE01020304 {
		t.Errorf("U64: got %v %v", v, err)
	}
	if v, err := sv2ReadBool(b, &off); err != nil || !v {
		t.Errorf("Bool: got %v %v", v, err)
	}
	if v, err := sv2ReadStr(b, &off); err != nil || v != "hello" {
		t.Errorf("Str: got %v %v", v, err)
	}
	if v, err := sv2ReadBytes(b, &off); err != nil || !bytes.Equal(v, []byte{0xDE, 0xAD}) {
		t.Errorf("Bytes: got %v %v", v, err)
	}
}

func TestSV2ErrUnexpectedEOF(t *testing.T) {
	b := []byte{0x01} // Only 1 byte, need 2 for U16
	off := 0
	_, err := sv2ReadU16(b, &off)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestSV2MessageRoundTrips(t *testing.T) {
	// SetupConnection
	sc := sv2SetupConnection{
		Protocol: 0, MinVersion: 2, MaxVersion: 2,
		Flags: 0, EndpointHost: "127.0.0.1", EndpointPort: 3333,
		Vendor: "test", HardwareVersion: "v1", Firmware: "fw1", DeviceID: "dev1",
	}
	encoded := sc.encode()
	var sc2 sv2SetupConnection
	if err := sc2.decode(encoded); err != nil {
		t.Fatal(err)
	}
	if sc2.Protocol != sc.Protocol || sc2.EndpointHost != sc.EndpointHost {
		t.Errorf("SetupConnection round-trip failed")
	}

	// SubmitSharesStandard
	ss := sv2SubmitSharesStandard{
		ChannelID: 1, SequenceNumber: 42, JobID: 7,
		Nonce: 0xABCD1234, NTime: 0x12345678, Version: 0x20000000,
	}
	enc := ss.encode()
	var ss2 sv2SubmitSharesStandard
	if err := ss2.decode(enc); err != nil {
		t.Fatal(err)
	}
	if ss2.Nonce != ss.Nonce || ss2.Version != ss.Version {
		t.Errorf("SubmitSharesStandard round-trip failed: %+v vs %+v", ss, ss2)
	}
}
