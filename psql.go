package dbsniff

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type PostgresServer struct {
	lAddr    string
	connStr  string
	shutdown chan struct{}
	listener net.Listener
}

func NewPostgresServer(lAddr, connStr string) (*PostgresServer, error) {
	s := &PostgresServer{
		lAddr:    lAddr,
		connStr:  connStr,
		shutdown: make(chan struct{}),
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}
	db.Close()

	// listen on lAddr
	l, err := net.Listen("tcp", lAddr)
	if err != nil {
		return nil, err
	}

	s.listener = l
	return s, nil
}

func (s *PostgresServer) Serve() {
	var (
		delay time.Duration
		wg    sync.WaitGroup
	)

OUTER:
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				break OUTER
			default:
			}
			if delay == 0 {
				delay = 5 * time.Millisecond
			} else {
				delay *= 2
			}
			if delay > time.Second {
				delay = time.Second
			}
			log.Printf("accept error: %v; retrying in %v", err, delay)
			time.Sleep(delay)
			continue
		}
		wg.Add(1)
		go s.serveClient(conn, &wg)
	}
}

func (s *PostgresServer) Shutdown() {
	close(s.shutdown)
	s.listener.Close()
}

func (s *PostgresServer) serveClient(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	brw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	conn.SetDeadline(time.Now().Add(time.Second * 10))
	err := s.recvStartupMessage(brw)
	if err != nil {
		log.Printf("error receiving startup message: %v", err)
		return
	}

	log.Printf("connected to %v", conn.RemoteAddr())

	db, err := sql.Open("postgres", s.connStr)
	if err != nil {
		log.Printf("error opening connection: %v", err)
		// TODO: send ErrorResponse with this error then Terminate
		return
	}

	err = db.Ping()
	if err != nil {
		log.Printf("error pinging connection: %v", err)
	}

	conn.SetDeadline(time.Time{})

	log.Printf("connected to upstream, sending ReadyForQuery")
	err = s.sendReadyForQuery(brw.Writer)
	if err != nil {
		log.Printf("error sending ReadyForQuery: %v", err)
		return
	}

	for {
		log.Printf("reading frame\n")
		ft, fl, err := readFrameHeader(brw.Reader)
		if err != nil {
			log.Printf("error reading frame header: %v", err)
			return
		}

		log.Printf("received frame: %v %v\n", ft, fl)
	}
}

func (s *PostgresServer) recvStartupMessage(brw *bufio.ReadWriter) error {
	var (
		frameLen uint32
		buf      readBuf
		err      error
	)

frameLoop:
	for {
		frameLen, err = readUint32(brw.Reader)
		if err != nil {
			return err
		}

		log.Printf("read startup message of length %v", frameLen) // TODO: DELME

		buf = readBuf(make([]byte, frameLen-4))
		_, err = io.ReadFull(brw.Reader, buf)
		if err != nil {
			return err
		}

		frameType, err := buf.uint32()
		if err != nil {
			return err
		}
		switch frameType {
		case uint32(196608):
			break frameLoop
		case uint32(80877103):
			// we don't really support TLS but we'll claim it and expect the client to
			// have ssl actually disabled
			// TODO: implement TLS
			brw.Writer.WriteByte('S')
		default:
			return fmt.Errorf("unexpected frame type 0x%X", frameType)
		}
	}

	for {
		k, err := buf.string()
		if err != nil {
			return err
		}
		if k == "" {
			return nil
		}
		v, err := buf.string()
		if err != nil {
			return fmt.Errorf("reading startup message param %s: %w", k, err)
		}
		log.Printf("startup message: %v=%v", k, v) // TODO: DELME
	}
}

func (s *PostgresServer) sendReadyForQuery(w io.Writer) error {
	buf := newWriteBuf(frameReadyForQuery)
	buf.byte('I')
	return buf.send(w)
}

type frameType byte

const (
	frameReadyForQuery frameType = 'Z'
)

func readFrameTypeHeader(br *bufio.Reader, wantType frameType) (frameLen uint32, err error) {
	gotType, frameLen, err := readFrameHeader(br)
	if err == nil && wantType != gotType {
		err = fmt.Errorf("unexpected frame type 0x%X, want 0x%X", gotType, wantType)
	}
	return frameLen, err
}

func readFrameHeader(br *bufio.Reader) (t frameType, frameLen uint32, err error) {
	tb, err := br.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	log.Printf("read frame type %v", tb) // TODO: DELME
	frameLen, err = readUint32(br)
	if err != nil {
		return 0, 0, err
	}
	log.Printf("read frame length %v", frameLen) // TODO: DELME
	return frameType(tb), frameLen, nil
}

var bin = binary.BigEndian

func readUint32(br *bufio.Reader) (uint32, error) {
	var u32 [4]byte
	for i := range &u32 {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		u32[i] = b
	}
	return bin.Uint32(u32[:]), nil
}
