package pingPong

import (
	"github.com/nm-morais/DeMMon/go-babel/pkg"
	"github.com/nm-morais/DeMMon/go-babel/pkg/message"
	"github.com/nm-morais/DeMMon/go-babel/pkg/peer"
	"github.com/nm-morais/DeMMon/go-babel/pkg/protocol"
	"github.com/nm-morais/DeMMon/go-babel/pkg/timer"
	"github.com/nm-morais/DeMMon/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"time"
)

const protoID = 1000

type PingPongProtocol struct {
	activePeers map[peer.Peer]interface{}
	contact     peer.Peer
	self        peer.Peer
}

func (m *PingPongProtocol) ID() protocol.ID {
	return protoID
}

func NewPingPongProtocol(contact peer.Peer, self peer.Peer) protocol.Protocol {
	return &PingPongProtocol{
		contact: contact,
		self:    self,
	}
}

func (m *PingPongProtocol) Init() {
	pkg.RegisterMessageHandler(protoID, Ping{}, m.handlePingMessage)
	pkg.RegisterMessageHandler(protoID, Pong{}, m.handlePongMessage)
	pkg.RegisterTimerHandler(protoID, PingTimer{}.ID(), m.handlePingTimer)
}

func (m *PingPongProtocol) Start() {
	if m.self != m.contact {
		pkg.Dial(m.contact, protoID, transport.NewTCPDialer())
	} else {
		log.Infof("I'm contact node")
	}
}

func (m *PingPongProtocol) DialFailed(peer peer.Peer) {
	if peer == m.contact {
		panic("contacting bootstrap node failed")
	}
}

func (m *PingPongProtocol) PeerDown(peer peer.Peer) {
	if peer == m.contact {
		panic("bootstrap node failed")
	}
}

func (m *PingPongProtocol) handlePingMessage(sender peer.Peer, msg message.Message) {
	pingMsg := msg.(Ping)
	log.Info("Got ping message, content : %s", pingMsg.Payload)

	response := Pong{Payload: pingMsg.Payload + "Pong"}
	pkg.SendMessage(response, sender, protoID, []protocol.ID{protoID})
}

func (m *PingPongProtocol) handlePongMessage(sender peer.Peer, msg message.Message) {
	pongMsg := msg.(Pong)
	log.Info("Got pong message, content : %s", pongMsg.Payload)
}

func (m *PingPongProtocol) handlePingTimer(timer timer.Timer) {
	log.Info("ping timer trigger")
	log.Infof("Sending pingMessage")
	pingMsg := Ping{Payload: "Ping"}

	if len(m.activePeers) == 0 {
		log.Infof("No active peers on ping timer")
	}

	for remotePeer := range m.activePeers {
		pkg.SendMessage(pingMsg, remotePeer, protoID, []protocol.ID{protoID})
	}

	pkg.RegisterTimer(protoID, PingTimer{timer: time.NewTimer(1 * time.Second)})
}

func (m *PingPongProtocol) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	if sourceProto == protoID {
		log.Infof("Dial successful")
		return true
	}
	return false
}

func (m *PingPongProtocol) InConnRequested(peer peer.Peer) bool {
	return true
}

func (m *PingPongProtocol) ConnEstablished(peer peer.Peer) {
	log.Infof("Connection established to peer %s", peer)
}
