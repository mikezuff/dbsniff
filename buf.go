package dbsniff

import (
	"bytes"
	"fmt"
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
