package rtmp

import (
	"crypto/rand"
	"errors"
	"io"
)

const (
	rtmpVersion     = 0x03
	handshakeLength = 1536
)

func serverHandshake(rw io.ReadWriter) error {
	var c0 [1]byte
	if _, err := io.ReadFull(rw, c0[:]); err != nil {
		return err
	}
	if c0[0] != rtmpVersion {
		return errors.New("bad rtmp version")
	}

	c1 := make([]byte, handshakeLength)
	if _, err := io.ReadFull(rw, c1); err != nil {
		return err
	}

	s0s1 := make([]byte, 1+handshakeLength)
	s0s1[0] = rtmpVersion
	if _, err := rand.Read(s0s1[1+8:]); err != nil {
		return err
	}
	if _, err := rw.Write(s0s1); err != nil {
		return err
	}
	if _, err := rw.Write(c1); err != nil {
		return err
	}

	c2 := make([]byte, handshakeLength)
	if _, err := io.ReadFull(rw, c2); err != nil {
		return err
	}
	return nil
}
