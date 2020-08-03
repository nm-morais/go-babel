package peer

import (
	"net"
)

type Peer interface {
	Addr() net.Addr
}

type peer struct {
	addr net.Addr
}

func NewPeer(addr net.Addr) Peer {
	return &peer{
		addr: addr,
	}
}

func (p *peer) Addr() net.Addr {
	return p.addr
}
