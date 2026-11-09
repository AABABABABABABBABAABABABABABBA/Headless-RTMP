package rtmp

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

const (
	amfNumber    = 0x00
	amfBoolean   = 0x01
	amfString    = 0x02
	amfObject    = 0x03
	amfNull      = 0x05
	amfUndefined = 0x06
	amfEcmaArray = 0x08
	amfObjectEnd = 0x09
	amfLongStr   = 0x0C
)

type amfValue = interface{}

type amfObjectMap map[string]amfValue

func decodeAMF(buf []byte) ([]amfValue, error) {
	r := &amfReader{buf: buf}
	var out []amfValue
	for r.pos < len(r.buf) {
		v, err := r.readValue()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

type amfReader struct {
	buf []byte
	pos int
}

func (r *amfReader) readByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *amfReader) readBytes(n int) ([]byte, error) {
	if r.pos+n > len(r.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

func (r *amfReader) readString(long bool) (string, error) {
	var n int
	if long {
		b, err := r.readBytes(4)
		if err != nil {
			return "", err
		}
		n = int(binary.BigEndian.Uint32(b))
	} else {
		b, err := r.readBytes(2)
		if err != nil {
			return "", err
		}
		n = int(binary.BigEndian.Uint16(b))
	}
	b, err := r.readBytes(n)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *amfReader) readValue() (amfValue, error) {
	t, err := r.readByte()
	if err != nil {
		return nil, err
	}
	switch t {
	case amfNumber:
		b, err := r.readBytes(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
	case amfBoolean:
		b, err := r.readByte()
		if err != nil {
			return nil, err
		}
		return b != 0, nil
	case amfString:
		return r.readString(false)
	case amfLongStr:
		return r.readString(true)
	case amfNull, amfUndefined:
		return nil, nil
	case amfObject:
		return r.readObject()
	case amfEcmaArray:
		if _, err := r.readBytes(4); err != nil {
			return nil, err
		}
		return r.readObject()
	case amfObjectEnd:
		return nil, errObjectEnd
	}
	return nil, errors.New("unsupported amf type")
}

var errObjectEnd = errors.New("object end")

func (r *amfReader) readObject() (amfObjectMap, error) {
	out := amfObjectMap{}
	for {
		nb, err := r.readBytes(2)
		if err != nil {
			return out, err
		}
		n := int(binary.BigEndian.Uint16(nb))
		if n == 0 {
			if _, err := r.readByte(); err != nil {
				return out, err
			}
			return out, nil
		}
		keyB, err := r.readBytes(n)
		if err != nil {
			return out, err
		}
		v, err := r.readValue()
		if err != nil {
			if errors.Is(err, errObjectEnd) {
				return out, nil
			}
			return out, err
		}
		out[string(keyB)] = v
	}
}

type amfWriter struct {
	buf []byte
}

func (w *amfWriter) writeString(s string) {
	w.buf = append(w.buf, amfString)
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	w.buf = append(w.buf, l[:]...)
	w.buf = append(w.buf, s...)
}

func (w *amfWriter) writeNumber(n float64) {
	w.buf = append(w.buf, amfNumber)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(n))
	w.buf = append(w.buf, b[:]...)
}

func (w *amfWriter) writeNull() {
	w.buf = append(w.buf, amfNull)
}

func (w *amfWriter) writeBool(v bool) {
	w.buf = append(w.buf, amfBoolean)
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

func (w *amfWriter) writeObject(m amfObjectMap) {
	w.buf = append(w.buf, amfObject)
	for k, v := range m {
		var l [2]byte
		binary.BigEndian.PutUint16(l[:], uint16(len(k)))
		w.buf = append(w.buf, l[:]...)
		w.buf = append(w.buf, k...)
		switch tv := v.(type) {
		case string:
			w.writeString(tv)
		case float64:
			w.writeNumber(tv)
		case int:
			w.writeNumber(float64(tv))
		case bool:
			w.writeBool(tv)
		case nil:
			w.writeNull()
		default:
			w.writeNull()
		}
	}
	w.buf = append(w.buf, 0, 0, amfObjectEnd)
}

func encodeAMFCommand(name string, txID float64, args ...amfValue) []byte {
	w := &amfWriter{}
	w.writeString(name)
	w.writeNumber(txID)
	for _, a := range args {
		switch tv := a.(type) {
		case string:
			w.writeString(tv)
		case float64:
			w.writeNumber(tv)
		case int:
			w.writeNumber(float64(tv))
		case bool:
			w.writeBool(tv)
		case nil:
			w.writeNull()
		case amfObjectMap:
			w.writeObject(tv)
		default:
			w.writeNull()
		}
	}
	return w.buf
}

func numOf(v amfValue) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}
func strOf(v amfValue) (string, bool) {
	s, ok := v.(string)
	return s, ok
}
