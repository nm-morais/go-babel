package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
)

const TCPTransportCaller = "TCPTransportCaller"

type TCPStream struct {
	listenAddr *net.TCPAddr
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
func (t *TCPStream) SetReadTimeout(deadline time.Time) errors.Error {
	if err := t.conn.SetReadDeadline(deadline); err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	return nil
}

func (t *TCPStream) Listen() (Listener, errors.Error) {
	l, err := net.ListenTCP("tcp4", t.listenAddr)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	return TCPListener{listener: l}, nil
}

func (t *TCPStream) DialWithTimeout(addr net.Addr, timeout time.Duration) errors.Error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr.String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	conn, err := net.DialTimeout("tcp4", tcpAddr.String(), timeout)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), TCPTransportCaller)
	}

	t.conn = conn.(*net.TCPConn)
	return nil
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
		conn: conn.(*net.TCPConn),
	}, nil
}
