package rtmp

import (
	"testing"
)

func TestAMFRoundTrip(t *testing.T) {
	payload := encodeAMFCommand("connect", 1, amfObjectMap{
		"app":            "live",
		"tcUrl":          "rtmp://localhost/live",
		"objectEncoding": 0.0,
	})
	vals, err := decodeAMF(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(vals) < 3 {
		t.Fatalf("want 3 vals, got %d", len(vals))
	}
	if s, _ := strOf(vals[0]); s != "connect" {
		t.Fatalf("want connect, got %q", s)
	}
	if n, _ := numOf(vals[1]); n != 1 {
		t.Fatalf("want tx=1, got %v", n)
	}
	obj, ok := vals[2].(amfObjectMap)
	if !ok {
		t.Fatalf("want object, got %T", vals[2])
	}
	if obj["app"] != "live" {
		t.Fatalf("want app=live, got %v", obj["app"])
	}
}

func TestAMFNumberString(t *testing.T) {
	payload := encodeAMFCommand("_result", 3.5, nil, 7.0)
	vals, err := decodeAMF(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(vals) != 4 {
		t.Fatalf("want 4 vals, got %d", len(vals))
	}
	if n, _ := numOf(vals[1]); n != 3.5 {
		t.Fatalf("want tx=3.5, got %v", n)
	}
	if vals[2] != nil {
		t.Fatalf("want nil, got %v", vals[2])
	}
	if n, _ := numOf(vals[3]); n != 7.0 {
		t.Fatalf("want 7, got %v", n)
	}
}
