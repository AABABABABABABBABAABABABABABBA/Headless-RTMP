package rtmp

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const (
	defaultChunkSize    = 128
	maxChunkSize        = 65536
	extendedTimestamp   = 0xFFFFFF
	defaultWindowAckSz  = 2500000
	defaultPeerBW       = 2500000
	streamMsgMaxPayload = 10 * 1024 * 1024
)

type message struct {
	csid      uint32
	typeID    uint8
	streamID  uint32
	timestamp uint32
	payload   []byte
}

type chunkStream struct {
	timestamp     uint32
	timestampDiff uint32
	length        uint32
	typeID        uint8
	streamID      uint32
	gotExt        bool
	readSoFar     uint32
	payload       []byte
}

type chunkReader struct {
	br         *bufio.Reader
	chunkSize  uint32
	streams    map[uint32]*chunkStream
	bytesRead  uint64
	lastAckAt  uint64
	ackWindow  uint32
}

func newChunkReader(r io.Reader) *chunkReader {
	return &chunkReader{
		br:        bufio.NewReaderSize(r, 32*1024),
		chunkSize: defaultChunkSize,
		streams:   map[uint32]*chunkStream{},
		ackWindow: defaultWindowAckSz,
	}
}

func (cr *chunkReader) setChunkSize(n uint32) {
	if n >= 128 && n <= maxChunkSize {
		cr.chunkSize = n
	}
}

func (cr *chunkReader) readBasicHeader() (fmt uint8, csid uint32, err error) {
	b, err := cr.br.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	cr.bytesRead++
	fmt = b >> 6
	csid = uint32(b & 0x3F)
	switch csid {
	case 0:
		b1, err := cr.br.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		cr.bytesRead++
		csid = uint32(b1) + 64
	case 1:
		var two [2]byte
		if _, err := io.ReadFull(cr.br, two[:]); err != nil {
			return 0, 0, err
		}
		cr.bytesRead += 2
		csid = uint32(two[0]) + uint32(two[1])*256 + 64
	}
	return fmt, csid, nil
}

func (cr *chunkReader) readMessage() (*message, error) {
	for {
		fmt, csid, err := cr.readBasicHeader()
		if err != nil {
			return nil, err
		}
		cs, ok := cr.streams[csid]
		if !ok {
			cs = &chunkStream{}
			cr.streams[csid] = cs
		}
		switch fmt {
		case 0:
			var h [11]byte
			if _, err := io.ReadFull(cr.br, h[:]); err != nil {
				return nil, err
			}
			cr.bytesRead += 11
			ts := uint32(h[0])<<16 | uint32(h[1])<<8 | uint32(h[2])
			cs.length = uint32(h[3])<<16 | uint32(h[4])<<8 | uint32(h[5])
			cs.typeID = h[6]
			cs.streamID = binary.LittleEndian.Uint32(h[7:11])
			if ts == extendedTimestamp {
				var ext [4]byte
				if _, err := io.ReadFull(cr.br, ext[:]); err != nil {
					return nil, err
				}
				cr.bytesRead += 4
				ts = binary.BigEndian.Uint32(ext[:])
				cs.gotExt = true
			} else {
				cs.gotExt = false
			}
			cs.timestamp = ts
			cs.timestampDiff = 0
		case 1:
			var h [7]byte
			if _, err := io.ReadFull(cr.br, h[:]); err != nil {
				return nil, err
			}
			cr.bytesRead += 7
			td := uint32(h[0])<<16 | uint32(h[1])<<8 | uint32(h[2])
			cs.length = uint32(h[3])<<16 | uint32(h[4])<<8 | uint32(h[5])
			cs.typeID = h[6]
			if td == extendedTimestamp {
				var ext [4]byte
				if _, err := io.ReadFull(cr.br, ext[:]); err != nil {
					return nil, err
				}
				cr.bytesRead += 4
				td = binary.BigEndian.Uint32(ext[:])
				cs.gotExt = true
			} else {
				cs.gotExt = false
			}
			cs.timestampDiff = td
			cs.timestamp += td
		case 2:
			var h [3]byte
			if _, err := io.ReadFull(cr.br, h[:]); err != nil {
				return nil, err
			}
			cr.bytesRead += 3
			td := uint32(h[0])<<16 | uint32(h[1])<<8 | uint32(h[2])
			if td == extendedTimestamp {
				var ext [4]byte
				if _, err := io.ReadFull(cr.br, ext[:]); err != nil {
					return nil, err
				}
				cr.bytesRead += 4
				td = binary.BigEndian.Uint32(ext[:])
				cs.gotExt = true
			} else {
				cs.gotExt = false
			}
			cs.timestampDiff = td
			cs.timestamp += td
		case 3:
			if cs.gotExt && cs.readSoFar == 0 {
				var ext [4]byte
				if _, err := io.ReadFull(cr.br, ext[:]); err != nil {
					return nil, err
				}
				cr.bytesRead += 4
				cs.timestamp = binary.BigEndian.Uint32(ext[:])
			}
			if cs.readSoFar == 0 {
				cs.timestamp += cs.timestampDiff
			}
		}

		if cs.length == 0 || cs.length > streamMsgMaxPayload {
			if cs.length > streamMsgMaxPayload {
				return nil, errors.New("message too large")
			}
		}
		if cs.payload == nil && cs.length > 0 {
			cs.payload = make([]byte, cs.length)
		}
		remaining := cs.length - cs.readSoFar
		toRead := cr.chunkSize
		if remaining < toRead {
			toRead = remaining
		}
		if toRead > 0 {
			if _, err := io.ReadFull(cr.br, cs.payload[cs.readSoFar:cs.readSoFar+toRead]); err != nil {
				return nil, err
			}
			cr.bytesRead += uint64(toRead)
			cs.readSoFar += toRead
		}
		if cs.readSoFar == cs.length {
			m := &message{
				csid:      csid,
				typeID:    cs.typeID,
				streamID:  cs.streamID,
				timestamp: cs.timestamp,
				payload:   cs.payload,
			}
			cs.payload = nil
			cs.readSoFar = 0
			return m, nil
		}
	}
}

type chunkWriter struct {
	bw        *bufio.Writer
	chunkSize uint32
}

func newChunkWriter(w io.Writer) *chunkWriter {
	return &chunkWriter{bw: bufio.NewWriterSize(w, 32*1024), chunkSize: defaultChunkSize}
}

func (cw *chunkWriter) flush() error { return cw.bw.Flush() }

func (cw *chunkWriter) setChunkSize(n uint32) { cw.chunkSize = n }

func (cw *chunkWriter) writeMessage(csid uint32, typeID uint8, streamID uint32, timestamp uint32, payload []byte) error {
	var basic [3]byte
	var bl int
	switch {
	case csid < 64:
		basic[0] = byte(csid)
		bl = 1
	case csid < 320:
		basic[0] = 0
		basic[1] = byte(csid - 64)
		bl = 2
	default:
		basic[0] = 1
		basic[1] = byte((csid - 64) & 0xFF)
		basic[2] = byte((csid - 64) >> 8)
		bl = 3
	}
	first := basic
	first[0] |= 0x00 << 6

	var hdr [11]byte
	ts := timestamp
	ext := ts >= extendedTimestamp
	if ext {
		hdr[0], hdr[1], hdr[2] = 0xFF, 0xFF, 0xFF
	} else {
		hdr[0] = byte(ts >> 16)
		hdr[1] = byte(ts >> 8)
		hdr[2] = byte(ts)
	}
	l := uint32(len(payload))
	hdr[3] = byte(l >> 16)
	hdr[4] = byte(l >> 8)
	hdr[5] = byte(l)
	hdr[6] = typeID
	binary.LittleEndian.PutUint32(hdr[7:11], streamID)

	if _, err := cw.bw.Write(first[:bl]); err != nil {
		return err
	}
	if _, err := cw.bw.Write(hdr[:]); err != nil {
		return err
	}
	if ext {
		var e [4]byte
		binary.BigEndian.PutUint32(e[:], ts)
		if _, err := cw.bw.Write(e[:]); err != nil {
			return err
		}
	}

	offset := uint32(0)
	for offset < l {
		end := offset + cw.chunkSize
		if end > l {
			end = l
		}
		if _, err := cw.bw.Write(payload[offset:end]); err != nil {
			return err
		}
		offset = end
		if offset < l {
			cont := basic
			cont[0] |= 0x03 << 6
			if _, err := cw.bw.Write(cont[:bl]); err != nil {
				return err
			}
			if ext {
				var e [4]byte
				binary.BigEndian.PutUint32(e[:], ts)
				if _, err := cw.bw.Write(e[:]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
