package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	. "github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

type Hyparview struct {
	babel          protocolManager.ProtocolManager
	contactNode    Peer
	activeView     []Peer
	passiveView    []Peer
	pendingDials   map[string]bool
	lastShuffleMsg ShuffleMessage
	timeStart      time.Time
	logger         *logrus.Logger
}

const joinTime = 10 * time.Second

const protoID = 2000
const activeViewSize = 5
const passiveViewSize = 25
const ARWL = 6
const PRWL = 3

const Ka = 3
const Kp = 2
const name = "Hyparview"

func NewHyparviewProtocol(contactNode Peer, babel protocolManager.ProtocolManager) protocol.Protocol {
	return &Hyparview{
		contactNode:  contactNode,
		activeView:   make([]Peer, 0, activeViewSize),
		passiveView:  make([]Peer, 0, passiveViewSize),
		pendingDials: make(map[string]bool),
		logger:       logs.NewLogger(name),
	}
}

func (h *Hyparview) ID() protocol.ID {
	return protoID
}

func (h *Hyparview) Name() string {
	return name
}

func (h *Hyparview) Logger() *logrus.Logger {
	return h.logger
}

func (h *Hyparview) Init() {
	rand.Seed(time.Now().Unix() + int64(rand.Int31()))
	h.babel.RegisterTimerHandler(protoID, ShuffleTimerID, h.HandleShuffleTimer)
	h.babel.RegisterMessageHandler(protoID, JoinMessage{}, h.HandleJoinMessage)
	h.babel.RegisterMessageHandler(protoID, ForwardJoinMessage{}, h.HandleForwardJoinMessage)
	h.babel.RegisterMessageHandler(protoID, ShuffleMessage{}, h.HandleShuffleMessage)
	h.babel.RegisterMessageHandler(protoID, ShuffleReplyMessage{}, h.HandleShuffleReplyMessage)
	h.babel.RegisterMessageHandler(protoID, NeighbourMessage{}, h.HandleNeighbourMessage)
	h.babel.RegisterMessageHandler(protoID, NeighbourMessageReply{}, h.HandleNeighbourReplyMessage)
	h.babel.RegisterMessageHandler(protoID, DisconnectMessage{}, h.HandleDisconnectMessage)
}

func (h *Hyparview) Start() {
	h.timeStart = time.Now()
	h.babel.RegisterTimer(h.ID(), ShuffleTimer{deadline: time.Now().Add(3 * time.Second)})
	if PeersEqual(h.babel.SelfPeer(), h.contactNode) {
		return
	}
	toSend := JoinMessage{}
	h.logger.Info("Sending join message...")
	h.babel.SendMessageSideStream(toSend, h.contactNode, h.contactNode.ToTCPAddr(), protoID, protoID)
}

func (h *Hyparview) InConnRequested(dialerProto protocol.ID, peer Peer) bool {
	if dialerProto != h.ID() {
		return false
	}

	if h.isPeerInView(peer, h.activeView) {
		return false
	}
	if h.isActiveViewFull() || len(h.activeView)+len(h.pendingDials) >= activeViewSize {
		return false
	}
	h.pendingDials[peer.String()] = true
	h.babel.Dial(h.ID(), peer, peer.ToTCPAddr())
	return true
}

func (h *Hyparview) OutConnDown(peer Peer) {
	h.dropPeerFromActiveView(peer.(Peer))
	h.logger.Errorf("Peer %s down", peer.String())
	h.logHyparviewState()
}

func (h *Hyparview) DialSuccess(sourceProto protocol.ID, peer Peer) bool {
	iPeer := peer.(Peer)
	delete(h.pendingDials, peer.String())
	if sourceProto != h.ID() {
		return false
	}

	if h.isPeerInView(peer, h.activeView) {
		h.logger.Info("Dialed node is already on active view")
		return true
	}

	h.dropPeerFromPassiveView(iPeer)
	if h.isActiveViewFull() {
		h.dropRandomElemFromActiveView()
	}

	h.addPeerToActiveView(iPeer)
	h.logHyparviewState()

	return true
}

func (h *Hyparview) DialFailed(peer Peer) {
	delete(h.pendingDials, peer.String())
	h.logger.Errorf("Failed to dial peer %s", peer.String())
	h.logHyparviewState()
}

func (h *Hyparview) MessageDelivered(message message.Message, peer Peer) {
	if message.Type() == DisconnectMessageType {
		h.babel.Disconnect(h.ID(), peer)
		h.logger.Infof("Disconnecting from %s", peer.String())
	}
	h.logger.Infof("Message %+v was sent to %s", message, peer.String())
}

func (h *Hyparview) MessageDeliveryErr(message message.Message, peer Peer, error errors.Error) {
	h.logger.Warnf("Message %s was not sent to %s because: %s", reflect.TypeOf(message), peer.String(), error.Reason())
}

// ---------------- Protocol handlers (messages) ----------------

func (h *Hyparview) HandleJoinMessage(sender Peer, message message.Message) {
	h.logger.Info("Received join message")
	if h.isActiveViewFull() {
		h.dropRandomElemFromActiveView()
	}
	iPeer := sender.(Peer)
	h.dialNodeToActiveView(iPeer)
	if len(h.activeView) > 0 {
		toSend := ForwardJoinMessage{
			TTL:            ARWL,
			OriginalSender: sender.(Peer),
		}
		for _, activePeer := range h.activeView {
			h.logger.Infof("Sending ForwardJoin (original=%s) message to: %s", sender.String(), activePeer.String())
			h.sendMessage(toSend, activePeer)
		}
	} else {
		h.logger.Warn("Did not send forwardJoin messages because i do not have enough peers")
	}
}

func (h *Hyparview) HandleForwardJoinMessage(sender Peer, message message.Message) {
	fwdJoinMsg := message.(ForwardJoinMessage)
	iPeer := sender.(Peer)
	h.logger.Infof("Received forward join message with ttl = %d from %s", fwdJoinMsg.TTL, sender.String())

	if fwdJoinMsg.TTL == 0 || len(h.activeView) == 1 {
		h.logger.Errorf("Accepting forwardJoin message from %s", fwdJoinMsg.OriginalSender.String())
		h.dialNodeToActiveView(fwdJoinMsg.OriginalSender)
		return
	}

	if fwdJoinMsg.TTL == PRWL {
		if !h.isPeerInView(fwdJoinMsg.OriginalSender, h.passiveView) && !h.isPeerInView(fwdJoinMsg.OriginalSender, h.activeView) {
			if h.isPassiveViewFull() {
				h.dropRandomElemFromPassiveView()
			}
			h.addPeerToPassiveView(fwdJoinMsg.OriginalSender)
		}
	}

	rndNodes := h.getRandomElementsFromView(1, h.activeView, fwdJoinMsg.OriginalSender, iPeer)
	if len(rndNodes) == 0 { // only know original sender, act as if join message
		h.logger.Errorf("Cannot forward forwardJoin message, dialing %s", fwdJoinMsg.OriginalSender.String())
		h.dialNodeToActiveView(fwdJoinMsg.OriginalSender)
		return
	}

	toSend := ForwardJoinMessage{
		TTL:            fwdJoinMsg.TTL - 1,
		OriginalSender: fwdJoinMsg.OriginalSender,
	}

	h.logger.Infof("Forwarding forwardJoin (original=%s) with TTL=%d message to : %s", fwdJoinMsg.OriginalSender.String(), toSend.TTL, rndNodes[0].String())
	h.sendMessage(toSend, rndNodes[0])
}

func (h *Hyparview) HandleNeighbourMessage(sender Peer, message message.Message) {
	h.logger.Info("Received neighbour message")
	neighbourMsg := message.(NeighbourMessage)
	if neighbourMsg.HighPrio {
		reply := NeighbourMessageReply{
			Accepted: true,
		}
		h.dropRandomElemFromActiveView()
		h.pendingDials[sender.String()] = true
		h.sendMessageTmpTransport(reply, sender)
	} else {
		if len(h.activeView) < activeViewSize {
			reply := NeighbourMessageReply{
				Accepted: true,
			}
			h.pendingDials[sender.String()] = true
			h.sendMessageTmpTransport(reply, sender)
		} else {
			reply := NeighbourMessageReply{
				Accepted: false,
			}
			h.sendMessageTmpTransport(reply, sender)
		}
	}
}

func (h *Hyparview) HandleNeighbourReplyMessage(sender Peer, message message.Message) {
	h.logger.Info("Received neighbour reply message")
	neighbourReplyMsg := message.(NeighbourMessageReply)
	if neighbourReplyMsg.Accepted {
		h.dialNodeToActiveView(sender.(Peer))
	}
}

func (h *Hyparview) HandleShuffleMessage(sender Peer, message message.Message) {
	//h.logger.Info("Received shuffle message")
	shuffleMsg := message.(ShuffleMessage)
	if shuffleMsg.TTL > 0 {
		if len(h.activeView) > 1 {
			toSend := ShuffleMessage{
				ID:    shuffleMsg.ID,
				TTL:   shuffleMsg.TTL - 1,
				Peers: shuffleMsg.Peers,
			}
			h.lastShuffleMsg = toSend
			rndNodes := h.getRandomElementsFromView(1, h.activeView, h.babel.SelfPeer().(Peer), sender.(Peer))
			if len(rndNodes) != 0 {
				//h.logger.Info("Forwarding shuffle message to :", rndNodes[0].ToString())
				h.sendMessage(toSend, rndNodes[0])
				return
			}
		}
	}
	//h.logger.Warn("Accepting shuffle message")
	// TTL is 0 or have no nodes to forward to
	// select random nr of hosts from passive view
	var exclusions []Peer
	for _, p := range shuffleMsg.Peers {
		exclusions = append(exclusions, p)
	}
	exclusions = append(exclusions, h.babel.SelfPeer().(Peer), sender.(Peer))

	toSend := h.getRandomElementsFromView(len(shuffleMsg.Peers), h.passiveView, exclusions...)
	for _, receivedHost := range shuffleMsg.Peers {
		if h.babel.SelfPeer().String() == receivedHost.String() {
			continue
		}

		if h.isPeerInView(receivedHost, h.activeView) || h.isPeerInView(receivedHost, h.passiveView) {
			continue
		}

		if h.isPassiveViewFull() { // if passive view is not full, skip check and add directly
			found := false
			for _, sentNode := range toSend {
				if h.dropPeerFromPassiveView(sentNode) {
					found = true
					break
				}
			}
			if !found {
				h.dropRandomElemFromPassiveView() // drop random element to make space
			}
		}
		h.addPeerToPassiveView(receivedHost)
	}
	reply := ShuffleReplyMessage{
		ID:    shuffleMsg.ID,
		Peers: toSend,
	}
	h.sendMessageTmpTransport(reply, sender)
}

func (h *Hyparview) HandleShuffleReplyMessage(sender Peer, m message.Message) {
	h.logger.Info("Received shuffle reply message")
	shuffleReplyMsg := m.(ShuffleReplyMessage)

	for _, receivedHost := range shuffleReplyMsg.Peers {

		if h.babel.SelfPeer().String() == receivedHost.String() {
			continue
		}

		if h.isPeerInView(receivedHost, h.activeView) || h.isPeerInView(receivedHost, h.passiveView) {
			continue
		}

		if h.isPassiveViewFull() { // if passive view is not full, skip check and add directly
			if shuffleReplyMsg.ID == h.lastShuffleMsg.ID {
				for i, sentPeer := range h.lastShuffleMsg.Peers {
					if h.dropPeerFromPassiveView(sentPeer) {
						h.lastShuffleMsg.Peers = append(h.lastShuffleMsg.Peers[:i], h.lastShuffleMsg.Peers[i:]...)
						break
					}
					h.lastShuffleMsg.Peers = append(h.lastShuffleMsg.Peers[:i], h.lastShuffleMsg.Peers[i:]...)
				}
			}
		}
		if h.isPassiveViewFull() { // view still full after trying to remove sent peers
			h.dropRandomElemFromPassiveView() // drop random element to make space
		}
		h.addPeerToPassiveView(receivedHost)
	}
}

// ---------------- Protocol handlers (timers) ----------------

func (h *Hyparview) HandleShuffleTimer(timer timer.Timer) {
	//h.logger.Info("Shuffle timer trigger")

	toWait := time.Duration(5000 * rand.Float32())
	h.babel.RegisterTimer(h.ID(), ShuffleTimer{deadline: time.Now().Add(toWait * time.Millisecond)})

	if time.Since(h.timeStart) > joinTime {
		if len(h.activeView) == 0 && len(h.passiveView) == 0 && !PeersEqual(h.babel.SelfPeer(), h.contactNode) {
			toSend := JoinMessage{}
			h.pendingDials[h.contactNode.String()] = true
			h.babel.SendMessageSideStream(toSend, h.contactNode, h.contactNode.ToUDPAddr(), protoID, protoID)
			return
		}
		if !h.isActiveViewFull() && len(h.pendingDials)+len(h.activeView) <= activeViewSize && len(h.passiveView) > 0 {
			h.logger.Warn("Promoting node from passive view to active view")
			aux := h.getRandomElementsFromView(1, h.passiveView)
			if len(aux) > 0 {
				h.dialNodeToActiveView(aux[0])
			}
		}
	}

	if len(h.activeView) == 0 {
		h.logger.Info("No nodes to send shuffle message message to")
		return
	}

	passiveViewRandomPeers := h.getRandomElementsFromView(Kp-1, h.passiveView)
	activeViewRandomPeers := h.getRandomElementsFromView(Ka, h.activeView)
	peers := append(passiveViewRandomPeers, activeViewRandomPeers...)
	peers = append(peers, h.babel.SelfPeer().(Peer))
	toSend := ShuffleMessage{
		TTL:   PRWL,
		Peers: peers,
	}
	h.lastShuffleMsg = toSend
	rndNode := h.activeView[rand.Intn(len(h.activeView))]
	h.logger.Info("Sending shuffle message to: ", rndNode.String())
	h.sendMessage(toSend, rndNode)
}

func (h *Hyparview) HandleDisconnectMessage(sender Peer, m message.Message) {
	h.logger.Warn("Got Disconnect message")
	iPeer := sender.(Peer)
	h.dropPeerFromActiveView(iPeer)
	if h.isPassiveViewFull() {
		h.dropRandomElemFromPassiveView()
	}
	h.addPeerToPassiveView(iPeer)
	h.babel.Disconnect(protoID, sender)
}

// ---------------- Auxiliary functions ----------------

func (h *Hyparview) logHyparviewState() {
	h.logger.Info("------------- Hyparview state -------------")
	var toLog string
	toLog = "Active view : "
	for _, p := range h.activeView {
		toLog += fmt.Sprintf("%s, ", p.String())
	}
	h.logger.Info(toLog)
	toLog = "Passive view : "
	for _, p := range h.passiveView {
		toLog += fmt.Sprintf("%s, ", p.String())
	}
	h.logger.Info(toLog)
	toLog = "Pending dials: "
	for p := range h.pendingDials {
		toLog += fmt.Sprintf("%s, ", p)
	}
	h.logger.Info(toLog)
	h.logger.Info("-------------------------------------------")
}

func (h *Hyparview) getRandomElementsFromView(amount int, view []Peer, exclusions ...Peer) []Peer {

	dest := make([]Peer, len(view))
	perm := rand.Perm(len(view))
	for i, v := range perm {
		dest[v] = view[i]
	}

	var toSend []Peer

	for i := 0; i < len(dest) && len(toSend) < amount; i++ {
		excluded := false
		curr := dest[i]
		for _, exclusion := range exclusions {
			if PeersEqual(exclusion, curr) { // skip exclusions
				excluded = true
				break
			}
		}
		if !excluded {
			toSend = append(toSend, curr)
		}
	}

	return toSend
}

func (h *Hyparview) isPeerInView(target Peer, view []Peer) bool {
	for _, p := range view {
		if PeersEqual(p, target) {
			return true
		}
	}
	return false
}

func (h *Hyparview) dropPeerFromActiveView(target Peer) bool {
	for i, p := range h.activeView {
		if PeersEqual(p, target) {
			h.activeView = append(h.activeView[:i], h.activeView[i+1:]...)
			return true
		}
	}
	return false
}

func (h *Hyparview) dropPeerFromPassiveView(target Peer) bool {
	for i, p := range h.passiveView {
		if PeersEqual(p, target) {
			h.passiveView = append(h.passiveView[:i], h.passiveView[i+1:]...)
			return true
		}
	}
	h.logHyparviewState()
	return false
}

func (h *Hyparview) dialNodeToActiveView(peer Peer) {
	if h.isPeerInView(peer, h.activeView) || h.pendingDials[peer.String()] {
		return
	}
	h.logger.Infof("dialing new node %s", peer.String())
	h.babel.Dial(h.ID(), peer, peer.ToTCPAddr())
	h.pendingDials[peer.String()] = true
	h.logHyparviewState()
}

func (h *Hyparview) addPeerToActiveView(newPeer Peer) {
	if PeersEqual(h.babel.SelfPeer(), newPeer) {
		panic("Trying to self to active view")
	}

	if h.isActiveViewFull() {
		panic("Cannot add node to active pool because it is full")
	}

	if h.isPeerInView(newPeer, h.activeView) {
		panic("Trying to add node already in view")
	}

	if h.isPeerInView(newPeer, h.passiveView) {
		panic("Trying to add node to active view already in passive view")
	}

	h.logger.Warnf("Added peer %s to active view", newPeer.String())
	h.activeView = append(h.activeView, newPeer)
	h.logHyparviewState()
}

func (h *Hyparview) addPeerToPassiveView(newPeer Peer) {

	if h.isPassiveViewFull() {
		panic("Trying to add node to view when view is full")
	}

	if PeersEqual(newPeer, h.babel.SelfPeer()) {
		panic("trying to add self to passive view ")
	}

	if h.isPeerInView(newPeer, h.passiveView) {
		panic("Trying to add node already in view")
	}

	if h.isPeerInView(newPeer, h.activeView) {
		panic("Trying to add node to passive view already in active view")
	}

	h.logger.Warnf("Added peer %s to passive view", newPeer.String())
	h.passiveView = append(h.passiveView, newPeer)
	h.logHyparviewState()
}

func (h *Hyparview) isActiveViewFull() bool {
	return len(h.activeView) >= activeViewSize
}

func (h *Hyparview) isPassiveViewFull() bool {
	return len(h.passiveView) >= passiveViewSize
}

func (h *Hyparview) dropRandomElemFromActiveView() {
	toRemove := rand.Intn(len(h.activeView))
	h.logger.Warnf("Dropping element %s from active view", h.activeView[toRemove].String())
	removed := h.activeView[toRemove]
	h.activeView = append(h.activeView[:toRemove], h.activeView[toRemove+1:]...)
	h.addPeerToPassiveView(removed)
	go func() {
		disconnectMsg := DisconnectMessage{}
		h.sendMessage(disconnectMsg, removed)
		h.babel.Disconnect(h.ID(), removed)
	}()
	h.logHyparviewState()
}

func (h *Hyparview) dropRandomElemFromPassiveView() {
	toRemove := rand.Intn(len(h.passiveView))
	h.logger.Warnf("Dropping element %s from passive view", h.passiveView[toRemove].String())
	h.passiveView = append(h.passiveView[:toRemove], h.passiveView[toRemove+1:]...)
	h.logHyparviewState()
}

func (h *Hyparview) sendMessage(msg message.Message, target Peer) {
	h.babel.SendMessage(msg, target, h.ID(), h.ID())
}

func (h *Hyparview) sendMessageTmpTransport(msg message.Message, target Peer) {
	h.babel.SendMessageSideStream(msg, target, target.ToTCPAddr(), h.ID(), h.ID())
}
