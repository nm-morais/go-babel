package transport

import (
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
	log "github.com/sirupsen/logrus"
	"net"
)

const TCPTransportCaller = "TCPTransportCaller"

type TCPTransport struct {
	ListenAddr net.Addr
	conn       net.Conn
	msgChan    chan []byte
}

const MaxMessageBytes = 2048

func NewTCPListener(listenAddr net.Addr) Transport {
	return &TCPTransport{
		ListenAddr: listenAddr,
		conn:       nil,
		msgChan:    make(chan []byte),
	}
}

func NewTCPDialer() Transport {
	return &TCPTransport{
		ListenAddr: nil,
		conn:       nil,
		msgChan:    make(chan []byte),
	}
}

func (t *TCPTransport) Listen() <-chan Transport {
	newConnChan := make(chan Transport)
	go func() {
		l, err := net.Listen("tcp4", t.ListenAddr.String())
		if err != nil {
			panic(err)
		}

		defer func() {
			if err := l.Close(); err != nil {
				panic(err)
			}
		}()

		for {
			conn, err := l.Accept()
			log.Infof("Someone dialed me")
			if err != nil {
				log.Error(err)
				continue
			}
			newTransport := &TCPTransport{
				ListenAddr: nil,
				conn:       conn,
				msgChan:    make(chan []byte),
			}
			newConnChan <- newTransport
		}
	}()
	return newConnChan
}

func (t *TCPTransport) SendMessage(msgBytes []byte) errors.Error {
	msgSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgSizeBytes, uint32(len(msgBytes)))
	_, err := t.conn.Write(append(msgSizeBytes, msgBytes...))
	if err != nil {
		return errors.FatalError(500, err.Error(), TCPTransportCaller)
	}
	return nil
}

func (t *TCPTransport) PipeBytesToChan() <-chan []byte {
	log.Info("Routine piping messages to chan has started")
	go func() {
		for {
			var lenBytes []byte
			for toRead := 4; toRead > 0; {
				lenBytesTmp := make([]byte, toRead)
				read, err := t.conn.Read(lenBytesTmp)
				if err != nil {
					log.Error("Routine piping messages to chan has exited due to:", err)
					close(t.msgChan)
					return
				}
				toRead -= read
				lenBytes = append(lenBytes, lenBytesTmp[:read]...)
			}
			var msgBytes []byte
			msgSize := int(binary.BigEndian.Uint32(lenBytes))
			log.Info("Message size: ", msgSize)
			for toRead := msgSize; toRead > 0; {
				msgBytesTmp := make([]byte, toRead)
				read, err := t.conn.Read(msgBytesTmp)
				if err != nil {
					log.Error("Routine piping messages to chan has exited due to:", err)
					close(t.msgChan)
					return
				}
				toRead -= read
				msgBytes = append(msgBytes, msgBytesTmp[:read]...)
			}
			t.msgChan <- msgBytes
		}
	}()
	return t.msgChan
}

func (t *TCPTransport) Dial(peer peer.Peer) <-chan errors.Error {
	errChan := make(chan errors.Error)
	go func() {
		addr, err := net.ResolveTCPAddr("tcp4", peer.Addr().String())
		if err != nil {
			errChan <- errors.NonFatalError(500, err.Error(), TCPTransportCaller)
			close(errChan)
			return
		}
		conn, err := net.DialTCP("tcp4", nil, addr)
		if err != nil {
			errChan <- errors.NonFatalError(500, err.Error(), TCPTransportCaller)
			close(errChan)
			return
		}
		t.conn = conn
		errChan <- nil
		close(errChan)
		return
	}()
	return errChan
}

func (t *TCPTransport) MessageChan() <-chan []byte {
	return t.msgChan
}

func (t *TCPTransport) Close() {
	if err := t.conn.Close(); err != nil { // TODO close nicely
		log.Error(err)
	}
}
