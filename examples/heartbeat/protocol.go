package heartbeat

import (
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/timer"
	"time"
)

const protoID = 1

type Heartbeat struct {
	activePeers map[peer.Peer]bool
	timerActive bool
}

func (m *Heartbeat) ID() protocol.ID {
	return protoID
}

func (m *Heartbeat) Init() {
	m.timerActive = false
	m.activePeers = make(map[peer.Peer]bool)
	pkg.RegisterMessageHandler(protoID, HeartbeatMessage{}, m.handleHeartbeat)
	pkg.RegisterTimerHandler(protoID, HeartbeatTimer{}.ID(), m.handleHeartbeatTimer)
}

func (m *Heartbeat) Start() {}

func (m *Heartbeat) DialFailed(peer peer.Peer) {}

func (m *Heartbeat) PeerDown(peer peer.Peer) {
	delete(m.activePeers, peer)
}

func (m *Heartbeat) handleHeartbeat(sender peer.Peer, msg message.Message) {
	//log.Infof("Got heartbeat from %s", sender.Addr().String())
}

func (m *Heartbeat) handleHeartbeatTimer(timer timer.Timer) {
	//log.Infof("ping timer trigger")
	m.timerActive = false
	hbMsg := HeartbeatMessage{}
	for remotePeer := range m.activePeers {
		//log.Infof("Sending heartbeat to %s", remotePeer.Addr().String())
		pkg.SendMessage(hbMsg, remotePeer, protoID, []protocol.ID{protoID})
	}
	if len(m.activePeers) > 0 && !m.timerActive {
		pkg.RegisterTimer(protoID, HeartbeatTimer{timer: time.NewTimer(1 * time.Second)})
		m.timerActive = true
	}
}

func (m *Heartbeat) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	//log.Infof("Connection established to peer %+v", peer.Addr().String())
	m.activePeers[peer] = true
	if len(m.activePeers) > 0 && !m.timerActive {
		pkg.RegisterTimer(protoID, HeartbeatTimer{timer: time.NewTimer(0 * time.Second)})
		m.timerActive = true
	}
	return true
}

func (m *Heartbeat) InConnRequested(peer peer.Peer) bool {
	m.activePeers[peer] = true
	if !m.timerActive {
		pkg.RegisterTimer(protoID, HeartbeatTimer{timer: time.NewTimer(0 * time.Second)})
		m.timerActive = true
	}
	return true
}
