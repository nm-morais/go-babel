package peer

import (
	"github.com/DeMMon/go-babel/pkg"
	"net"
)

type IPeer interface {
	ID() pkg.ID
	Addr() net.Addr
}

type Peer struct {
	id   pkg.ID
	addr net.Addr
}

func NewPeer(id pkg.ID, addr net.Addr) *Peer {
	return &Peer{
		id:   id,
		addr: addr,
	}
}

func (p Peer) ID() pkg.ID {
	return p.id
}

func (p Peer) Addr() net.Addr {
	return p.Addr()
}
