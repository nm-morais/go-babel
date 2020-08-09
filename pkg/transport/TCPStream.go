package transport

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

const TCPTransportCaller = "TCPTransportCaller"

type TCPStream struct {
	listenAddr net.Addr
	conn       *net.TCPConn
	l          net.Listener
	mw         *MessageWriter
	mr         *MessageReader
}

func NewTCPListener(listenAddr net.Addr) Stream {
	return &TCPStream{listenAddr: listenAddr}
}

func NewTCPDialer(listenAddr net.Addr) Stream {
	return &TCPStream{}
}

func (t *TCPStream) ListenAddr() net.Addr {
	return t.listenAddr
}

func (t *TCPStream) SetReadTimeout(duration time.Duration) {
	//log.Info("Setting readTimeout for conn ", t.conn.RemoteAddr().String())
	if err := t.conn.SetReadDeadline(time.Now().Add(duration)); err != nil {
		log.Error(err)
	}
}

func (t *TCPStream) Listen() (Listener, errors.Error) {
	l, err := net.Listen("tcp4", t.listenAddr.String())
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	return TCPListener{listener: l}, nil
}

func (t *TCPStream) Dial(peer peer.Peer) errors.Error {
	addr, err := net.ResolveTCPAddr("tcp4", peer.Addr().String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	conn, err := net.DialTCP("tcp4", nil, addr)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}

	t.conn = conn
	t.mr = NewMessageReader(conn)
	t.mw = NewMessageWriter(conn)

	return nil
}

func (t *TCPStream) Write(msgBytes []byte) (int, error) {
	return t.mw.Write(msgBytes)
}

func (t *TCPStream) Read(msgBytes []byte) (int, error) {
	return t.mr.Read(msgBytes)
}

func (t *TCPStream) Close() errors.Error {
	if err := t.conn.Close(); err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	return nil
}

type TCPListener struct {
	listener net.Listener
}

func (l TCPListener) Listener() Listener {
	return l
}

func (l TCPListener) Accept() (Stream, errors.Error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	mr := NewMessageReader(conn)
	mw := NewMessageWriter(conn)
	return &TCPStream{
		listenAddr: l.listener.Addr(),
		conn:       conn.(*net.TCPConn),
		mw:         mw,
		mr:         mr,
	}, nil
}
