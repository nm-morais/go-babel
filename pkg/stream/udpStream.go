package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
)

const UDPTransportCaller = "UDPTransportCaller"

type UDPStream struct {
	listenAddr *net.UDPAddr
	targetAddr *net.UDPAddr
	packetConn net.Conn
}

func NewUDPListener(listenAddr *net.UDPAddr) Stream {
	return &UDPStream{listenAddr: listenAddr}
}

func NewUDPDialer() Stream {
	return &UDPStream{}
}

func (t *UDPStream) ListenAddr() net.Addr {
	return t.listenAddr
}

func (t *UDPStream) SetReadTimeout(duration time.Duration) {
	panic("not supported")
}

func (t *UDPStream) Listen() (Listener, errors.Error) {
	return UDPListener{addr: t.listenAddr}, nil
}

func (t *UDPStream) Dial(toDial net.Addr) errors.Error {
	resolved, err := net.ResolveUDPAddr("udp", toDial.String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), UDPTransportCaller)
	}
	t.targetAddr = resolved
	conn, err := net.Dial("udp", resolved.String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), UDPTransportCaller)
	}
	t.packetConn = conn
	return nil
}

func (t *UDPStream) Write(msgBytes []byte) (int, error) {
	return t.packetConn.Write(msgBytes)
}

func (t *UDPStream) Read(msgBytes []byte) (int, error) {
	return t.packetConn.Read(msgBytes)
}

func (t *UDPStream) Close() error {
	return t.packetConn.Close()
}

type UDPListener struct {
	addr *net.UDPAddr
}

func (l UDPListener) Listener() Listener {
	return l
}

func (l UDPListener) Accept() (Stream, errors.Error) {
	conn, err := net.ListenUDP("udp", l.addr)
	if err != nil {
		panic(err)
	}
	return &UDPStream{listenAddr: l.addr, packetConn: conn}, nil
}
