package membership

import (
	"github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/protocol"
)

const protoID = 1000

type MembershipPotocol struct {
	pm *pkg.ProtocolManager
}

func (m *MembershipPotocol) ID() protocol.ID {
	return protoID
}

func (m *MembershipPotocol) Init() {
	m.pm = pkg.GetProtocolManager()
}

func (m *MembershipPotocol) Start() {
}

func (m *MembershipPotocol) InConnRequested(peer peer.Peer) bool {
	panic("implement me")
}

func (m *MembershipPotocol) InConnEstablished(peer peer.Peer) bool {
	panic("implement me")
}

func (m *MembershipPotocol) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	panic("implement me")
}

func (m *MembershipPotocol) DialFailed(peer peer.Peer) {
	panic("implement me")
}

func (m *MembershipPotocol) PeerDown(peer peer.Peer) {
	panic("implement me")
}

func handlePingMessage(msg message.Message) {
}

func handlePongMessage(msg message.Message) {

}
