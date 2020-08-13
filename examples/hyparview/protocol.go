package hyparview

import (
	"fmt"
	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

type Hyparview struct {
	contactNode    peer.Peer
	activeView     []peer.Peer
	passiveView    []peer.Peer
	pendingDials   map[string]bool
	lastShuffleMsg ShuffleMessage
	timeStart      time.Time
	logger         *log.Logger
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

func NewHYparviewProtocol(contactNode peer.Peer) protocol.Protocol {
	return &Hyparview{
		contactNode:  contactNode,
		activeView:   make([]peer.Peer, 0, activeViewSize),
		passiveView:  make([]peer.Peer, 0, passiveViewSize),
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

func (h *Hyparview) Logger() *log.Logger {
	return h.logger
}

func (h *Hyparview) Init() {
	rand.Seed(time.Now().Unix() + int64(rand.Int31()))
	pkg.RegisterTimerHandler(protoID, ShuffleTimerID, h.HandleShuffleTimer)
	pkg.RegisterMessageHandler(protoID, JoinMessage{}, h.HandleJoinMessage)
	pkg.RegisterMessageHandler(protoID, ForwardJoinMessage{}, h.HandleForwardJoinMessage)
	pkg.RegisterMessageHandler(protoID, ShuffleMessage{}, h.HandleShuffleMessage)
	pkg.RegisterMessageHandler(protoID, ShuffleReplyMessage{}, h.HandleShuffleReplyMessage)
	pkg.RegisterMessageHandler(protoID, NeighbourMessage{}, h.HandleNeighbourMessage)
	pkg.RegisterMessageHandler(protoID, NeighbourMessageReply{}, h.HandleNeighbourReplyMessage)
	pkg.RegisterMessageHandler(protoID, DisconnectMessage{}, h.HandleDisconnectMessage)
}

func (h *Hyparview) Start() {
	h.timeStart = time.Now()
	pkg.RegisterTimer(h.ID(), ShuffleTimer{timer: time.NewTimer(3 * time.Second)})
	if h.contactNode.Equals(pkg.SelfPeer()) {
		return
	}
	toSend := JoinMessage{}
	log.Info("Sending join message...")
	pkg.SendMessageSideStream(toSend, h.contactNode, protoID, []protocol.ID{protoID}, stream.NewTCPDialer())
}

func (h *Hyparview) InConnRequested(peer peer.Peer) bool {
	if h.isPeerInView(peer, h.activeView) {
		return false
	}
	if h.isActiveViewFull() || len(h.activeView)+len(h.pendingDials) >= activeViewSize {
		return false
	}
	h.pendingDials[peer.ToString()] = true
	pkg.Dial(peer, h.ID(), stream.NewTCPDialer())
	return true
}

func (h *Hyparview) OutConnDown(peer peer.Peer) {
	h.dropPeerFromActiveView(peer)
	h.logger.Errorf("Peer %s down", peer.ToString())
	h.logHyparviewState()
}

func (h *Hyparview) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	delete(h.pendingDials, peer.ToString())
	if sourceProto != h.ID() {
		return false
	}

	if h.isPeerInView(peer, h.activeView) {
		h.logger.Info("Dialed node is already on active view")
		return true
	}

	h.dropPeerFromPassiveView(peer)
	if h.isActiveViewFull() {
		h.dropRandomElemFromActiveView()
	}

	h.addPeerToActiveView(peer)
	h.logHyparviewState()

	pkg.MeasureLatencyTo(5, peer, h.printMeasurements)

	return true
}

func (h *Hyparview) printMeasurements(peer peer.Peer, measurements []time.Duration) {
	h.logger.Infof("Measurements to %s", peer.ToString())
	for i := 0; i < len(measurements); i++ {
		h.logger.Infof("%d : %d microsec", i, measurements[i].Microseconds())
	}
}

func (h *Hyparview) DialFailed(peer peer.Peer) {
	delete(h.pendingDials, peer.ToString())
	h.logger.Errorf("Failed to dial peer %s", peer.ToString())
	h.logHyparviewState()
}

// ---------------- Protocol handlers (messages) ----------------

func (h *Hyparview) HandleJoinMessage(sender peer.Peer, message message.Message) {
	h.logger.Info("Received join message")
	if h.isActiveViewFull() {
		h.dropRandomElemFromActiveView()
	}
	h.dialNodeToActiveView(sender)
	if len(h.activeView) > 0 {
		toSend := ForwardJoinMessage{
			TTL:            ARWL,
			OriginalSender: sender,
		}
		for _, activePeer := range h.activeView {
			h.logger.Infof("Sending ForwardJoin (original=%s) message to: %s", sender.ToString(), activePeer.ToString())
			h.sendMessage(toSend, activePeer)
		}
	} else {
		h.logger.Warn("Did not send forwardJoin messages because i do not have enough peers")
	}
}

func (h *Hyparview) HandleForwardJoinMessage(sender peer.Peer, message message.Message) {
	fwdJoinMsg := message.(ForwardJoinMessage)
	h.logger.Infof("Received forward join message with ttl = %d from %s", fwdJoinMsg.TTL, sender.ToString())

	if fwdJoinMsg.TTL == 0 || len(h.activeView) == 1 {
		h.logger.Errorf("Accepting forwardJoin message from %s", fwdJoinMsg.OriginalSender.ToString())
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

	rndNodes := h.getRandomElementsFromView(1, h.activeView, fwdJoinMsg.OriginalSender, sender)
	if len(rndNodes) == 0 { // only know original sender, act as if join message
		h.logger.Errorf("Cannot forward forwardJoin message, dialing %s", fwdJoinMsg.OriginalSender.ToString())
		h.dialNodeToActiveView(fwdJoinMsg.OriginalSender)
		return
	}

	toSend := ForwardJoinMessage{
		TTL:            fwdJoinMsg.TTL - 1,
		OriginalSender: fwdJoinMsg.OriginalSender,
	}

	h.logger.Infof("Forwarding forwardJoin (original=%s) with TTL=%d message to : %s", fwdJoinMsg.OriginalSender.ToString(), toSend.TTL, rndNodes[0].ToString())
	h.sendMessage(toSend, rndNodes[0])
}

func (h *Hyparview) HandleNeighbourMessage(sender peer.Peer, message message.Message) {
	h.logger.Info("Received neighbour message")
	neighbourMsg := message.(NeighbourMessage)
	if neighbourMsg.HighPrio {
		reply := NeighbourMessageReply{
			Accepted: true,
		}
		h.dropRandomElemFromActiveView()
		h.pendingDials[sender.ToString()] = true
		h.sendMessageTmpTransport(reply, sender)
	} else {
		if len(h.activeView) < activeViewSize {
			reply := NeighbourMessageReply{
				Accepted: true,
			}
			h.pendingDials[sender.ToString()] = true
			h.sendMessageTmpTransport(reply, sender)
		} else {
			reply := NeighbourMessageReply{
				Accepted: false,
			}
			h.sendMessageTmpTransport(reply, sender)
		}
	}
}

func (h *Hyparview) HandleNeighbourReplyMessage(sender peer.Peer, message message.Message) {
	h.logger.Info("Received neighbour reply message")
	neighbourReplyMsg := message.(NeighbourMessageReply)
	if neighbourReplyMsg.Accepted {
		h.dialNodeToActiveView(sender)
	}
}

func (h *Hyparview) HandleShuffleMessage(sender peer.Peer, message message.Message) {
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
			rndNodes := h.getRandomElementsFromView(1, h.activeView, pkg.SelfPeer(), sender)
			if len(rndNodes) != 0 {
				//h.logger.Info("Forwarding shuffle message to :", rndNodes[0].ToString())
				h.sendMessage(toSend, rndNodes[0])
				return
			} else {
				//h.logger.Error("Could not forward shuffle message")
			}
		}
	}
	//h.logger.Warn("Accepting shuffle message")
	// TTL is 0 or have no nodes to forward to
	// select random nr of hosts from passive view
	exclusions := append(shuffleMsg.Peers, pkg.SelfPeer(), sender)
	toSend := h.getRandomElementsFromView(len(shuffleMsg.Peers), h.passiveView, exclusions...)
	for _, receivedHost := range shuffleMsg.Peers {
		if pkg.SelfPeer().ToString() == receivedHost.ToString() {
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

func (h *Hyparview) HandleShuffleReplyMessage(sender peer.Peer, m message.Message) {
	h.logger.Info("Received shuffle reply message")
	shuffleReplyMsg := m.(ShuffleReplyMessage)

	for _, receivedHost := range shuffleReplyMsg.Peers {

		if pkg.SelfPeer().ToString() == receivedHost.ToString() {
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
	pkg.RegisterTimer(h.ID(), ShuffleTimer{timer: time.NewTimer(5 * time.Second)})

	if time.Since(h.timeStart) > joinTime {
		if len(h.activeView) == 0 && len(h.passiveView) == 0 && !h.contactNode.Equals(pkg.SelfPeer()) {
			toSend := JoinMessage{}
			h.pendingDials[h.contactNode.ToString()] = true
			pkg.SendMessageSideStream(toSend, h.contactNode, protoID, []protocol.ID{protoID}, stream.NewTCPDialer())
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
	peers = append(peers, pkg.SelfPeer())
	toSend := ShuffleMessage{
		TTL:   PRWL,
		Peers: peers,
	}
	h.lastShuffleMsg = toSend
	rndNode := h.activeView[rand.Intn(len(h.activeView))]
	h.logger.Info("Sending shuffle message to: ", rndNode.ToString())
	h.sendMessage(toSend, rndNode)
}

func (h *Hyparview) HandleDisconnectMessage(sender peer.Peer, m message.Message) {
	h.logger.Warn("Got Disconnect message")
	h.dropPeerFromActiveView(sender)
	if h.isPassiveViewFull() {
		h.dropRandomElemFromPassiveView()
	}
	h.addPeerToPassiveView(sender)
	pkg.Disconnect(protoID, sender)
}

// ---------------- Auxiliary functions ----------------

func (h *Hyparview) logHyparviewState() {
	h.logger.Info("------------- Hyparview state -------------")
	var toLog string
	toLog = "Active view : "
	for _, p := range h.activeView {
		toLog += fmt.Sprintf("%s, ", p.ToString())
	}
	h.logger.Info(toLog)
	toLog = "Passive view : "
	for _, p := range h.passiveView {
		toLog += fmt.Sprintf("%s, ", p.ToString())
	}
	h.logger.Info(toLog)
	toLog = "Pending dials: "
	for p, _ := range h.pendingDials {
		toLog += fmt.Sprintf("%s, ", p)
	}
	h.logger.Info(toLog)
	h.logger.Info("-------------------------------------------")
}

func (h *Hyparview) getRandomElementsFromView(amount int, view []peer.Peer, exclusions ...peer.Peer) []peer.Peer {

	dest := make([]peer.Peer, len(view))
	perm := rand.Perm(len(view))
	for i, v := range perm {
		dest[v] = view[i]
	}

	var toSend []peer.Peer

	for i := 0; i < len(dest) && len(toSend) < amount; i++ {
		excluded := false
		curr := dest[i]
		for _, exclusion := range exclusions {
			if exclusion.Equals(curr) { // skip exclusions
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

func (h *Hyparview) isPeerInView(target peer.Peer, view []peer.Peer) bool {
	for _, p := range view {
		if p.Equals(target) {
			return true
		}
	}
	return false
}

func (h *Hyparview) dropPeerFromActiveView(target peer.Peer) bool {
	for i, p := range h.activeView {
		if p.Equals(target) {
			h.activeView = append(h.activeView[:i], h.activeView[i+1:]...)
			return true
		}
	}
	return false
}

func (h *Hyparview) dropPeerFromPassiveView(target peer.Peer) bool {
	for i, p := range h.passiveView {
		if p.Equals(target) {
			h.passiveView = append(h.passiveView[:i], h.passiveView[i+1:]...)
			return true
		}
	}
	h.logHyparviewState()
	return false
}

func (h *Hyparview) dialNodeToActiveView(peer peer.Peer) {
	if h.isPeerInView(peer, h.activeView) || h.pendingDials[peer.ToString()] {
		return
	}
	h.logger.Infof("dialing new node %s", peer.ToString())
	pkg.Dial(peer, h.ID(), stream.NewTCPDialer())
	h.pendingDials[peer.ToString()] = true
	h.logHyparviewState()
}

func (h *Hyparview) addPeerToActiveView(newPeer peer.Peer) {
	if newPeer.Equals(pkg.SelfPeer()) {
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

	h.logger.Warnf("Added peer %s to active view", newPeer.ToString())
	h.activeView = append(h.activeView, newPeer)
	h.logHyparviewState()
}

func (h *Hyparview) addPeerToPassiveView(newPeer peer.Peer) {

	if h.isPassiveViewFull() {
		panic("Trying to add node to view when view is full")
	}

	if newPeer.Equals(pkg.SelfPeer()) {
		panic("trying to add self to passive view ")
	}

	if h.isPeerInView(newPeer, h.passiveView) {
		panic("Trying to add node already in view")
	}

	if h.isPeerInView(newPeer, h.activeView) {
		panic("Trying to add node to passive view already in active view")
	}

	h.logger.Warnf("Added peer %s to passive view", newPeer.ToString())
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
	h.logger.Warnf("Dropping element %s from active view", h.activeView[toRemove].ToString())
	removed := h.activeView[toRemove]
	h.activeView = append(h.activeView[:toRemove], h.activeView[toRemove+1:]...)
	h.addPeerToPassiveView(removed)
	go func() {
		disconnectMsg := DisconnectMessage{}
		<-h.sendMessage(disconnectMsg, removed)
		pkg.Disconnect(h.ID(), removed)
	}()
	h.logHyparviewState()
}

func (h *Hyparview) dropRandomElemFromPassiveView() {
	toRemove := rand.Intn(len(h.passiveView))
	h.logger.Warnf("Dropping element %s from passive view", h.passiveView[toRemove].ToString())
	h.passiveView = append(h.passiveView[:toRemove], h.passiveView[toRemove+1:]...)
	h.logHyparviewState()
}

func (h *Hyparview) sendMessage(msg message.Message, target peer.Peer) chan interface{} {
	return pkg.SendMessage(msg, target, h.ID(), []protocol.ID{h.ID()})
}

func (h *Hyparview) sendMessageTmpTransport(msg message.Message, target peer.Peer) {
	pkg.SendMessageSideStream(msg, target, h.ID(), []protocol.ID{h.ID()}, stream.NewTCPDialer())
}
