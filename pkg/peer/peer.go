package peer

import (
	"net"
)

type Peer interface {
	Addr() net.Addr
	Equals(other Peer) bool
	ToString() string
}

type peer struct {
	addr net.Addr
}

func NewPeer(addr net.Addr) Peer {
	return peer{
		addr: addr,
	}
}

func (p peer) Addr() net.Addr {
	return p.addr
}

func (p peer) ToString() string {
	return p.addr.String()
}

func (p peer) Equals(otherPeer Peer) bool {
	return p.ToString() == otherPeer.ToString()
}
