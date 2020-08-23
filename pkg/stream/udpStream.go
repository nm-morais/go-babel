package stream

import (
	"net"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/peer"
	log "github.com/sirupsen/logrus"
)

const UDPTransportCaller = "UDPTransportCaller"

type UDPStream struct {
	listenAddr net.Addr
	conn       *net.UDPConn
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
	panic("not supported")
}

func (t *UDPStream) Dial(peer peer.Peer) errors.Error {
	addr, err := net.ResolveUDPAddr("udp", peer.Addr().String())
	if err != nil {
		return errors.NonFatalError(500, err.Error(), UDPTransportCaller)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), UDPTransportCaller)
	}
	t.conn = conn
	return nil
}

func (t *UDPStream) Write(msgBytes []byte) (int, error) {
	return t.conn.Write(msgBytes)
}

func (t *UDPStream) Read(msgBytes []byte) (int, error) {

	conn, err := net.ListenPacket("udp", t.listenAddr.String())
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(err)
		}
	}()
	if err != nil {
		panic(err)
	}

	n, _, err := conn.ReadFrom(msgBytes)

	if err != nil {
		return -1, err
	}

	return n, nil
}

func (t *UDPStream) Close() error {
	return t.conn.Close()
}
