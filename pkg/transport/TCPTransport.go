package transport

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
	log "github.com/sirupsen/logrus"
	"net"
)

const TCPTransportCaller = "TCPTransportCaller"

type TCPTransport struct {
	ListenAddr net.Addr
	conn       net.Conn
	peer       peer.Peer
	msgChan    chan []byte
}

const MaxMessageBytes = 2048

func NewTCPListener(listenAddr net.Addr) Transport {
	return &TCPTransport{
		ListenAddr: listenAddr,
		conn:       nil,
		peer:       nil,
	}
}

func NewTCPDialer() Transport {
	return &TCPTransport{
		ListenAddr: nil,
		conn:       nil,
		peer:       nil,
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
			}
			newTransport := &TCPTransport{
				ListenAddr: nil,
				conn:       conn,
				peer:       peer.NewPeer(conn.RemoteAddr()),
			}
			newConnChan <- newTransport
		}
	}()
	return newConnChan
}

func (t *TCPTransport) SendMessage(msgBytes []byte) errors.Error {
	_, err := t.conn.Write(msgBytes)
	if err != nil {
		return errors.FatalError(500, err.Error(), TCPTransportCaller)
	}
	return nil
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

func (t *TCPTransport) PipeBytesToChan() <-chan []byte {
	msgChan := make(chan []byte)
	t.msgChan = msgChan
	log.Info("Routine piping messages to chan has started")
	go func() {
		for {
			msgBytes := make([]byte, MaxMessageBytes)
			read, err := t.conn.Read(msgBytes)
			if err != nil {

				log.Error("Routine piping messages to chan has exited due to:", err)
				close(msgChan)
				return
			}
			msgChan <- msgBytes[:read]
		}
	}()
	return msgChan
}

func (t *TCPTransport) Peer() peer.Peer {
	return t.peer
}

func (t *TCPTransport) MessageChan() <-chan []byte {
	return t.msgChan
}

func (t *TCPTransport) Close() {
	if err := t.conn.Close(); err != nil { // TODO close nicely
		log.Error(err)
	}
}
