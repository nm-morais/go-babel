package main

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const PingMessageType message.ID = 1001
const PongMessageType message.ID = 1002

type Ping struct {
	Payload string
}

type PingSerializer struct {
}

var pingSerializer = PingSerializer{}

func (Ping) Type() message.ID {
	return PingMessageType
}

func (msg Ping) Serializer() message.Serializer {
	return pingSerializer
}

func (msg Ping) Deserializer() message.Deserializer {
	return pingSerializer
}

func (PingSerializer) Serialize(message message.Message) []byte {
	return []byte(message.(Ping).Payload)

}

func (PingSerializer) Deserialize(toDeserialize []byte) message.Message {
	return Ping{string(toDeserialize)}
}

type Pong struct {
	Payload string
}

type PongSerializer struct {
}

var pongSerializer = PongSerializer{}

func (Pong) Type() message.ID {
	return PongMessageType
}

func (msg Pong) Serializer() message.Serializer {
	return pongSerializer
}

func (msg Pong) Deserializer() message.Deserializer {
	return pongSerializer
}

func (PongSerializer) Serialize(message message.Message) []byte {
	return []byte(message.(Pong).Payload)
}

func (PongSerializer) Deserialize(toDeserialize []byte) message.Message {
	return Pong{Payload: string(toDeserialize)}
}

const protoID = 1000
const name = "PingPong"

type PingPongProtocol struct {
	activePeers map[string]peer.Peer
	contact     peer.Peer
	logger      *logrus.Logger
	babel       protocolManager.ProtocolManager
}

func NewPingPongProtocol(contact peer.Peer, babel protocolManager.ProtocolManager) protocol.Protocol {
	return &PingPongProtocol{
		babel:       babel,
		contact:     contact,
		activePeers: make(map[string]peer.Peer),
		logger:      logs.NewLogger(name),
	}
}

func (m *PingPongProtocol) MessageDelivered(message message.Message, peer peer.Peer) {
	m.logger.Debugf("Message %+v was sent to %s", message, peer.String())
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

func (m *PingPongProtocol) Logger() *logrus.Logger {
	return m.logger
}

func (m *PingPongProtocol) Init() {
	m.activePeers = make(map[string]peer.Peer)
	m.babel.RegisterMessageHandler(protoID, Ping{}, m.handlePingMessage)
	m.babel.RegisterMessageHandler(protoID, Pong{}, m.handlePongMessage)
	m.babel.RegisterTimerHandler(protoID, PingTimer{}.ID(), m.handlePingTimer)
}

func (m *PingPongProtocol) Start() {
	if !peer.PeersEqual(m.babel.SelfPeer(), m.contact) {
		m.logger.Infof("Dialing contact node")
		m.babel.Dial(protoID, m.contact, m.contact.ToTCPAddr())
	} else {
		m.logger.Infof("I'm contact node")
	}
}

func (m *PingPongProtocol) DialFailed(p peer.Peer) {
	m.logger.Infof("Dial failed")
	if peer.PeersEqual(m.contact, p) {
		panic("contacting bootstrap node failed")
	}
}

func (m *PingPongProtocol) handlePingMessage(sender peer.Peer, msg message.Message) {
	pingMsg := msg.(Ping)
	m.logger.Infof("Got ping message, content : %s", pingMsg.Payload)
	response := Pong{Payload: pingMsg.Payload + "Pong"}
	m.logger.Infof("Sending Pong message, content : %s", response.Payload)
	m.babel.SendMessage(response, sender, protoID, protoID, false)
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

	for _, remotePeer := range m.activePeers {
		m.logger.Infof("Sending pingMessage to %s", remotePeer.String())
		for i := 0; i < 1; i++ {
			m.babel.SendMessage(pingMsg, remotePeer, protoID, protoID, false)
		}
	}

}

func (m *PingPongProtocol) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	m.logger.Infof("Connection established to peer %+v", peer.String())
	if sourceProto == protoID {
		m.activePeers[peer.String()] = peer
		m.babel.RegisterPeriodicTimer(protoID, PingTimer{duration: 3 * time.Second})
		return true
	}
	return false
}

func (m *PingPongProtocol) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return dialerProto != m.ID()
}

func (m *PingPongProtocol) OutConnDown(peer peer.Peer) {
	delete(m.activePeers, peer.String())
}
