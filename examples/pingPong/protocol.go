package pingPong

import (
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/nm-morais/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"time"
)

const protoID = 1000

type PingPongProtocol struct {
	activePeers map[peer.Peer]bool
	contact     peer.Peer
}

func (m *PingPongProtocol) ID() protocol.ID {
	return protoID
}

func NewPingPongProtocol(contact peer.Peer) protocol.Protocol {
	return &PingPongProtocol{
		contact: contact,
	}
}

func (m *PingPongProtocol) Init() {
	m.activePeers = make(map[peer.Peer]bool)
	pkg.RegisterMessageHandler(protoID, Ping{}, m.handlePingMessage)
	pkg.RegisterMessageHandler(protoID, Pong{}, m.handlePongMessage)
	pkg.RegisterTimerHandler(protoID, PingTimer{}.ID(), m.handlePingTimer)
}

func (m *PingPongProtocol) Start() {
	if pkg.Addr().String() != m.contact.Addr().String() {
		log.Infof("Dialing contact node")
		pkg.Dial(m.contact, protoID, transport.NewTCPDialer())
	} else {
		log.Infof("I'm contact node")
	}
}

func (m *PingPongProtocol) DialFailed(peer peer.Peer) {
	log.Infof("Dial failed")
	if peer.Addr().String() == m.contact.Addr().String() {
		panic("contacting bootstrap node failed")
	}
}

func (m *PingPongProtocol) PeerDown(peer peer.Peer) {
	delete(m.activePeers, peer)
}

func (m *PingPongProtocol) handlePingMessage(sender peer.Peer, msg message.Message) {
	pingMsg := msg.(Ping)
	log.Infof("Got ping message, content : %s", pingMsg.Payload)
	response := Pong{Payload: pingMsg.Payload + "Pong"}
	log.Infof("Sending Pong message, content : %s", response.Payload)
	pkg.SendMessage(response, sender, protoID, []protocol.ID{protoID})
}

func (m *PingPongProtocol) handlePongMessage(sender peer.Peer, msg message.Message) {
	pongMsg := msg.(Pong)
	log.Infof("Got pong message, content : %s", pongMsg.Payload)
}

func (m *PingPongProtocol) handlePingTimer(timer timer.Timer) {
	//log.Infof("ping timer trigger")
	pingMsg := Ping{Payload: "Ping"}
	if len(m.activePeers) == 0 {
		log.Infof("No active peers on ping timer")
	}

	for remotePeer := range m.activePeers {
		log.Infof("Sending pingMessage to %s", remotePeer.Addr().String())
		//pkg.SendMessage(pingMsg, remotePeer, protoID, []protocol.ID{protoID})
		pkg.SendMessageTempTransport(pingMsg, remotePeer, protoID, []protocol.ID{protoID}, transport.NewTCPDialer())
	}

	pkg.RegisterTimer(protoID, PingTimer{timer: time.NewTimer(1 * time.Second)})
}

func (m *PingPongProtocol) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	log.Infof("Connection established to peer %+v", peer.Addr().String())
	if sourceProto == protoID {
		log.Infof("Dial successful")
		m.activePeers[peer] = true
		pkg.RegisterTimer(protoID, PingTimer{timer: time.NewTimer(0 * time.Second)})
		return true
	}
	return false
}

func (m *PingPongProtocol) InConnRequested(peer peer.Peer) bool {
	return true
}
