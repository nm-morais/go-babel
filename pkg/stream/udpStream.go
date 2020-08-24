package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const UDPTransportCaller = "UDPTransportCaller"

type UDPStream struct {
	listenAddr net.Addr
	packetConn net.Conn
	targetAddr net.Addr
}

func NewUDPListener(listenAddr net.Addr) Stream {
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
	conn, err := net.ListenPacket("udp", t.listenAddr.String())
	if err != nil {
		panic(err)
	}
	t.packetConn = conn.(net.Conn)
	return nil, nil
}

func (t *UDPStream) Dial(peer peer.Peer) errors.Error {
	addr, err := net.ResolveUDPAddr("udp", peer.Addr().String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), UDPTransportCaller)
	}
	t.targetAddr = addr
	conn, err := net.Dial("udp", addr.String())
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
