package rtmp

import (
	"bytes"
	"testing"
)

func TestChunkRoundTripSmall(t *testing.T) {
	var buf bytes.Buffer
	cw := newChunkWriter(&buf)
	payload := []byte("hello rtmp")
	if err := cw.writeMessage(3, msgAMF0Command, 0, 100, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cw.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	cr := newChunkReader(&buf)
	m, err := cr.readMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if m.typeID != msgAMF0Command {
		t.Fatalf("type: %d", m.typeID)
	}
	if m.timestamp != 100 {
		t.Fatalf("ts: %d", m.timestamp)
	}
	if !bytes.Equal(m.payload, payload) {
		t.Fatalf("payload mismatch: %q vs %q", m.payload, payload)
	}
}

func TestChunkRoundTripLarge(t *testing.T) {
	var buf bytes.Buffer
	cw := newChunkWriter(&buf)
	cw.setChunkSize(4096)
	payload := bytes.Repeat([]byte("ABCD"), 5000)
	if err := cw.writeMessage(4, msgVideo, 1, 12345, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cw.flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	cr := newChunkReader(&buf)
	cr.setChunkSize(4096)
	m, err := cr.readMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if m.typeID != msgVideo {
		t.Fatalf("type: %d", m.typeID)
	}
	if m.streamID != 1 {
		t.Fatalf("sid: %d", m.streamID)
	}
	if m.timestamp != 12345 {
		t.Fatalf("ts: %d", m.timestamp)
	}
	if !bytes.Equal(m.payload, payload) {
		t.Fatalf("payload mismatch (len %d vs %d)", len(m.payload), len(payload))
	}
}

func TestChunkMultipleMessages(t *testing.T) {
	var buf bytes.Buffer
	cw := newChunkWriter(&buf)
	payloads := [][]byte{
		[]byte("one"),
		[]byte("two-two"),
		bytes.Repeat([]byte("x"), 400),
	}
	for i, p := range payloads {
		if err := cw.writeMessage(4, msgAudio, 1, uint32(i*100), p); err != nil {
			t.Fatal(err)
		}
	}
	if err := cw.flush(); err != nil {
		t.Fatal(err)
	}
	cr := newChunkReader(&buf)
	for i, expect := range payloads {
		m, err := cr.readMessage()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(m.payload, expect) {
			t.Fatalf("msg %d mismatch (len %d vs %d)", i, len(m.payload), len(expect))
		}
	}
}
