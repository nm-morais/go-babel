package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	log "github.com/sirupsen/logrus"
)

const protoID = 1000
const name = "PingPong"

type PingPongProtocol struct {
	activePeers map[peer.Peer]bool
	contact     peer.Peer
	logger      *log.Logger
}

func NewPingPongProtocol(contact peer.Peer) protocol.Protocol {
	return &PingPongProtocol{
		contact: contact,
		logger:  logs.NewLogger(name),
	}
}

func (m *PingPongProtocol) MessageDelivered(message message.Message, peer peer.Peer) {
	m.logger.Debugf("Message %+v was sent to %s", message, peer.ToString())
}

func (m *PingPongProtocol) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	m.logger.Warnf("Message %+v was not sent to %s due to:", message, peer, error.ToString())
}

func (m *PingPongProtocol) ID() protocol.ID {
	return protoID
}

func (m *PingPongProtocol) Name() string {
	return name
}

func (m *PingPongProtocol) Logger() *log.Logger {
	return m.logger
}

func (m *PingPongProtocol) Init() {
	m.activePeers = make(map[peer.Peer]bool)
	pkg.RegisterMessageHandler(protoID, Ping{}, m.handlePingMessage)
	pkg.RegisterMessageHandler(protoID, Pong{}, m.handlePongMessage)
	pkg.RegisterTimerHandler(protoID, PingTimer{}.ID(), m.handlePingTimer)
}

func (m *PingPongProtocol) Start() {
	if pkg.SelfPeer().Addr().String() != m.contact.Addr().String() {
		m.logger.Infof("Dialing contact node")
		pkg.Dial(m.contact, protoID, stream.NewTCPDialer())
	} else {
		m.logger.Infof("I'm contact node")
	}
}

func (m *PingPongProtocol) DialFailed(peer peer.Peer) {
	m.logger.Infof("Dial failed")
	if peer.Addr().String() == m.contact.Addr().String() {
		panic("contacting bootstrap node failed")
	}
}

func (m *PingPongProtocol) handlePingMessage(sender peer.Peer, msg message.Message) {
	pingMsg := msg.(Ping)
	m.logger.Infof("Got ping message, content : %s", pingMsg.Payload)
	response := Pong{Payload: pingMsg.Payload + "Pong"}
	m.logger.Infof("Sending Pong message, content : %s", response.Payload)
	pkg.SendMessage(response, sender, protoID, []protocol.ID{protoID})
}

func (m *PingPongProtocol) handlePongMessage(sender peer.Peer, msg message.Message) {
	pongMsg := msg.(Pong)
	m.logger.Infof("Got pong message, content : %s", pongMsg.Payload)
}

func (m *PingPongProtocol) handlePingTimer(timer timer.Timer) {
	//m.logger.Infof("ping timer trigger")
	pingMsg := Ping{Payload: "Ping"}
	if len(m.activePeers) == 0 {
		m.logger.Infof("No active peers on ping timer")
		return
	}

	for remotePeer := range m.activePeers {
		m.logger.Infof("Sending pingMessage to %s", remotePeer.Addr().String())
		//pkg.Write(pingMsg, remotePeer, protoID, []protocol.ID{protoID})
		for i := 0; i < 1; i++ {
			pkg.SendMessage(pingMsg, remotePeer, protoID, []protocol.ID{protoID})
		}
	}

	pkg.RegisterTimer(protoID, PingTimer{timer: time.NewTimer(1 * time.Second)})
}

func (m *PingPongProtocol) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	m.logger.Infof("Connection established to peer %+v", peer.Addr().String())
	if sourceProto == protoID {
		m.activePeers[peer] = true
		pkg.RegisterTimer(protoID, PingTimer{timer: time.NewTimer(0 * time.Second)})
		return true
	}
	return false
}

func (m *PingPongProtocol) InConnRequested(peer peer.Peer) bool {
	return true
}

func (m *PingPongProtocol) OutConnDown(peer peer.Peer) {
	delete(m.activePeers, peer)
}
