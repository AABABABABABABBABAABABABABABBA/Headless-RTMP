package rtmp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aababababababbabaabababababba/headless-rtmp/internal/config"
	"github.com/aababababababbabaabababababba/headless-rtmp/internal/hub"
)

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := &config.Config{
		RTMPAddr:      addr,
		HTTPAddr:      "127.0.0.1:0",
		App:           "live",
		ReadTimeoutS:  10,
		WriteTimeoutS: 10,
		ChunkSize:     4096,
		GOPCache:      true,
	}
	h := hub.New(log.New(os.Stderr, "[test] ", log.LstdFlags))
	srv := NewServer(cfg, h, log.New(os.Stderr, "[test] ", log.LstdFlags))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
	}()

	for i := 0; i < 50; i++ {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Cleanup(func() {
		cancel()
		sctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		srv.Shutdown(sctx)
	})
	return srv, addr
}

type testClient struct {
	c  net.Conn
	cr *chunkReader
	cw *chunkWriter
}

func dialClient(t *testing.T, addr string) *testClient {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if err := clientHandshake(c); err != nil {
		c.Close()
		t.Fatalf("handshake: %v", err)
	}
	return &testClient{c: c, cr: newChunkReader(c), cw: newChunkWriter(c)}
}

func clientHandshake(c net.Conn) error {
	var c0 [1]byte
	c0[0] = rtmpVersion
	if _, err := c.Write(c0[:]); err != nil {
		return err
	}
	c1 := make([]byte, handshakeLength)
	if _, err := rand.Read(c1[8:]); err != nil {
		return err
	}
	if _, err := c.Write(c1); err != nil {
		return err
	}
	var s0 [1]byte
	if _, err := io.ReadFull(c, s0[:]); err != nil {
		return err
	}
	if s0[0] != rtmpVersion {
		return errors.New("bad server version")
	}
	s1 := make([]byte, handshakeLength)
	if _, err := io.ReadFull(c, s1); err != nil {
		return err
	}
	s2 := make([]byte, handshakeLength)
	if _, err := io.ReadFull(c, s2); err != nil {
		return err
	}
	if _, err := c.Write(s1); err != nil {
		return err
	}
	return nil
}

func (tc *testClient) sendCommand(csid uint32, streamID uint32, payload []byte) error {
	if err := tc.cw.writeMessage(csid, msgAMF0Command, streamID, 0, payload); err != nil {
		return err
	}
	return tc.cw.flush()
}

func (tc *testClient) readUntilCommand(name string) ([]interface{}, error) {
	for {
		m, err := tc.cr.readMessage()
		if err != nil {
			return nil, err
		}
		if m.typeID == msgSetChunkSize {
			tc.cr.setChunkSize(binary.BigEndian.Uint32(m.payload[:4]) & 0x7FFFFFFF)
			continue
		}
		if m.typeID != msgAMF0Command {
			continue
		}
		vals, err := decodeAMF(m.payload)
		if err != nil {
			return nil, err
		}
		if len(vals) == 0 {
			continue
		}
		n, _ := strOf(vals[0])
		if name == "" || n == name {
			return vals, nil
		}
	}
}

func (tc *testClient) readUntilStatus(code string) ([]interface{}, error) {
	for {
		vals, err := tc.readUntilCommand("")
		if err != nil {
			return nil, err
		}
		n, _ := strOf(vals[0])
		if n != "onStatus" || len(vals) < 4 {
			continue
		}
		obj, ok := vals[3].(amfObjectMap)
		if !ok {
			continue
		}
		if c, _ := obj["code"].(string); c == code {
			return vals, nil
		}
	}
}

func TestConnectHandshake(t *testing.T) {
	_, addr := startTestServer(t)
	tc := dialClient(t, addr)
	defer tc.c.Close()

	payload := encodeAMFCommand("connect", 1, amfObjectMap{
		"app":            "live",
		"tcUrl":          "rtmp://127.0.0.1/live",
		"objectEncoding": 0.0,
	})
	if err := tc.sendCommand(csidCommand, 0, payload); err != nil {
		t.Fatal(err)
	}
	vals, err := tc.readUntilCommand("_result")
	if err != nil {
		t.Fatalf("read _result: %v", err)
	}
	if tx, _ := numOf(vals[1]); tx != 1 {
		t.Fatalf("want tx=1, got %v", tx)
	}
	info, ok := vals[3].(amfObjectMap)
	if !ok {
		t.Fatalf("want info object, got %T", vals[3])
	}
	if info["code"] != "NetConnection.Connect.Success" {
		t.Fatalf("bad code: %v", info["code"])
	}
}

func TestPublishAndPlay(t *testing.T) {
	_, addr := startTestServer(t)

	pub := dialClient(t, addr)
	defer pub.c.Close()

	if err := pub.sendCommand(csidCommand, 0, encodeAMFCommand("connect", 1, amfObjectMap{"app": "live"})); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.readUntilCommand("_result"); err != nil {
		t.Fatalf("pub connect: %v", err)
	}
	if err := pub.sendCommand(csidCommand, 0, encodeAMFCommand("createStream", 2, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.readUntilCommand("_result"); err != nil {
		t.Fatalf("pub createStream: %v", err)
	}
	if err := pub.sendCommand(csidCommand, 1, encodeAMFCommand("publish", 3, nil, "test", "live")); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.readUntilStatus("NetStream.Publish.Start"); err != nil {
		t.Fatalf("pub publish.start: %v", err)
	}

	videoSeqHdr := []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x42, 0xC0, 0x1E}
	if err := pub.cw.writeMessage(csidVideo, msgVideo, 1, 0, videoSeqHdr); err != nil {
		t.Fatal(err)
	}
	pub.cw.flush()

	play := dialClient(t, addr)
	defer play.c.Close()

	if err := play.sendCommand(csidCommand, 0, encodeAMFCommand("connect", 1, amfObjectMap{"app": "live"})); err != nil {
		t.Fatal(err)
	}
	if _, err := play.readUntilCommand("_result"); err != nil {
		t.Fatalf("play connect: %v", err)
	}
	if err := play.sendCommand(csidCommand, 0, encodeAMFCommand("createStream", 2, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := play.readUntilCommand("_result"); err != nil {
		t.Fatalf("play createStream: %v", err)
	}
	if err := play.sendCommand(csidCommand, 1, encodeAMFCommand("play", 3, nil, "test")); err != nil {
		t.Fatal(err)
	}
	if _, err := play.readUntilStatus("NetStream.Play.Start"); err != nil {
		t.Fatalf("play.start: %v", err)
	}

	videoFrame := []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF}
	if err := pub.cw.writeMessage(csidVideo, msgVideo, 1, 33, videoFrame); err != nil {
		t.Fatal(err)
	}
	pub.cw.flush()

	play.c.SetReadDeadline(time.Now().Add(3 * time.Second))
	gotSeq := false
	gotFrame := false
	for i := 0; i < 10 && (!gotSeq || !gotFrame); i++ {
		m, err := play.cr.readMessage()
		if err != nil {
			t.Fatalf("play read: %v", err)
		}
		if m.typeID == msgVideo {
			if len(m.payload) >= 2 && m.payload[1] == 0x00 {
				gotSeq = true
			}
			if len(m.payload) >= 2 && m.payload[1] == 0x01 {
				gotFrame = true
			}
		}
	}
	if !gotSeq {
		t.Errorf("did not receive video sequence header")
	}
	if !gotFrame {
		t.Errorf("did not receive video frame")
	}
}

func TestPlayerDisconnectKeepsPublisher(t *testing.T) {
	_, addr := startTestServer(t)

	pub := dialClient(t, addr)
	defer pub.c.Close()

	if err := pub.sendCommand(csidCommand, 0, encodeAMFCommand("connect", 1, amfObjectMap{"app": "live"})); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.readUntilCommand("_result"); err != nil {
		t.Fatal(err)
	}
	if err := pub.sendCommand(csidCommand, 0, encodeAMFCommand("createStream", 2, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.readUntilCommand("_result"); err != nil {
		t.Fatal(err)
	}
	if err := pub.sendCommand(csidCommand, 1, encodeAMFCommand("publish", 3, nil, "keeptest", "live")); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.readUntilStatus("NetStream.Publish.Start"); err != nil {
		t.Fatal(err)
	}
	pub.cw.writeMessage(csidVideo, msgVideo, 1, 0, []byte{0x17, 0x00, 0x00, 0x00, 0x00})
	pub.cw.flush()

	play := dialClient(t, addr)
	play.sendCommand(csidCommand, 0, encodeAMFCommand("connect", 1, amfObjectMap{"app": "live"}))
	play.readUntilCommand("_result")
	play.sendCommand(csidCommand, 0, encodeAMFCommand("createStream", 2, nil))
	play.readUntilCommand("_result")
	play.sendCommand(csidCommand, 1, encodeAMFCommand("play", 3, nil, "keeptest"))
	if _, err := play.readUntilStatus("NetStream.Play.Start"); err != nil {
		t.Fatal(err)
	}
	play.c.Close()
	time.Sleep(200 * time.Millisecond)

	if err := pub.cw.writeMessage(csidVideo, msgVideo, 1, 33, []byte{0x17, 0x01, 0x00, 0x00, 0x00, 0xAA}); err != nil {
		t.Fatalf("publisher write failed after player disconnect: %v", err)
	}
	pub.cw.flush()

	play2 := dialClient(t, addr)
	defer play2.c.Close()
	play2.sendCommand(csidCommand, 0, encodeAMFCommand("connect", 1, amfObjectMap{"app": "live"}))
	play2.readUntilCommand("_result")
	play2.sendCommand(csidCommand, 0, encodeAMFCommand("createStream", 2, nil))
	play2.readUntilCommand("_result")
	play2.sendCommand(csidCommand, 1, encodeAMFCommand("play", 3, nil, "keeptest"))
	if _, err := play2.readUntilStatus("NetStream.Play.Start"); err != nil {
		t.Fatalf("second player got Play.Start error — publisher state was dropped when first player left: %v", err)
	}
}
