package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const TCPTransportCaller = "TCPTransportCaller"

type TCPStream struct {
	listenAddr net.Addr
	conn       *net.TCPConn
}

func NewTCPListener(listenAddr *net.TCPAddr) Stream {
	return &TCPStream{listenAddr: listenAddr}
}

func NewTCPDialer() Stream {
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

func (t *TCPStream) Dial(addr net.Addr) errors.Error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr.String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	conn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}

	t.conn = conn

	return nil
}

func (t *TCPStream) Write(msgBytes []byte) (int, error) {
	return t.conn.Write(msgBytes)
}

func (t *TCPStream) Read(msgBytes []byte) (int, error) {
	return t.conn.Read(msgBytes)
}

func (t *TCPStream) Close() error {
	return t.conn.Close()
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
	return &TCPStream{
		listenAddr: l.listener.Addr(),
		conn:       conn.(*net.TCPConn),
	}, nil
}
