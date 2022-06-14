package dbsniff

import (
	"bytes"
	"fmt"
	"io"
	"log"
)

type readBuf []byte

func (b *readBuf) uint32() (uint32, error) {
	if len(*b) < 4 {
		return 0, fmt.Errorf("invalid message format; expected uint32")
	}
	v := bin.Uint32((*b)[:4])
	*b = (*b)[4:]
	return v, nil
}

func (b *readBuf) string() (string, error) {
	i := bytes.IndexByte(*b, 0)
	if i < 0 {
		return "", fmt.Errorf("invalid message format; expected string terminator")
	}
	s := (*b)[:i]
	*b = (*b)[i+1:]
	return string(s), nil
}

func (b *readBuf) len() int {
	return len(*b)
}

type writeBuf struct {
	buf []byte
	pos int
}

func newWriteBuf(ft frameType) *writeBuf {
	return &writeBuf{
		buf: []byte{byte(ft), 0, 0, 0, 0},
	}
}

func (b *writeBuf) byte(c byte) {
	b.buf = append(b.buf, c)
}

func (b *writeBuf) send(w io.Writer) error {
	p := b.buf[1:]
	bin.PutUint32(p, uint32(len(p)))
	log.Printf("sending %v", b.buf) // DELME
	_, err := w.Write(b.buf)
	return err
}
