package pkg

import (
	"container/heap"
	"io"
	"math"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/analytics"
	"github.com/nm-morais/go-babel/pkg/dataStructures"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

const nodeWatcherCaller = "NodeWatcher"

type ConditionFunc func(NodeInfo) bool

type Condition struct {
	Peer                      peer.Peer
	EvalConditionTickDuration time.Duration
	CondFunc                  ConditionFunc
	ProtoId                   protocol.ID
	Notification              notification.Notification
	Repeatable                bool
	EnableGracePeriod         bool
	GracePeriod               time.Duration
}

type NodeInfo struct {
	mw               io.Writer
	mr               io.Reader
	nrRetries        int
	subscribers      map[protocol.ID]bool
	peer             peer.Peer
	peerConn         net.Conn
	closeOnce        *sync.Once
	nrMessagesNoWait int
	enoughSamples    chan interface{}
	LatencyCalc      *analytics.LatencyCalculator
	Detector         *analytics.Detector
}

type cancelCondReq struct {
	key     int
	removed chan int
}

type NodeWatcherImpl struct {
	selfPeer peer.Peer
	conf     NodeWatcherConf

	watching     map[string]NodeInfo
	watchingLock *sync.RWMutex

	conditions     dataStructures.PriorityQueue
	addCondChan    chan *dataStructures.Item
	cancelCondChan chan *cancelCondReq

	logger *logrus.Logger
}

type NodeWatcherConf struct {
	EvalConditionTickDuration time.Duration
	MaxRedials                int
	TcpTestTimeout            time.Duration
	UdpTestTimeout            time.Duration
	NrTestMessagesToSend      int
	NrMessagesWithoutWait     int
	NrTestMessagesToReceive   int
	HbTickDuration            time.Duration
	WindowSize                int
	MinSamplesFaultDetector   int
	MinSamplesLatencyEstimate int
	OldLatencyWeight          float32
	NewLatencyWeight          float32
}

type NodeWatcher interface {
	Watch(Peer peer.Peer, protoID protocol.ID) errors.Error
	WatchWithInitialLatencyValue(p peer.Peer, issuerProto protocol.ID, latency time.Duration) errors.Error
	Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error
	GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error)
	GetNodeInfoWithDeadline(peer peer.Peer, deadline time.Time) (*NodeInfo, errors.Error)
	NotifyOnCondition(c Condition) (int, errors.Error)
	CancelCond(condID int) errors.Error
	Logger() *logrus.Logger
}

func NewNodeWatcher(selfPeer peer.Peer, config NodeWatcherConf) NodeWatcher {
	nm := &NodeWatcherImpl{
		conf:         config,
		selfPeer:     selfPeer,
		watching:     make(map[string]NodeInfo),
		watchingLock: &sync.RWMutex{},
		conditions:   make(dataStructures.PriorityQueue, 0),
		logger:       logs.NewLogger(nodeWatcherCaller),
	}

	if nm.conf.OldLatencyWeight+nm.conf.NewLatencyWeight != 1 {
		panic("OldLatencyWeight + NewLatencyWeight != 1")
	}

	nm.logger.Infof("My configs: %+v", config)
	nm.start()
	return nm
}

func (nm *NodeWatcherImpl) dialAndWatch(issuerProto protocol.ID, p peer.Peer) {
	stream, err := nm.establishStreamTo(p)
	if err != nil {
		return
	}
	nm.watchingLock.Lock()
	curr := nm.watching[p.ToString()]
	curr.peerConn = stream
	curr.mr = messageIO.NewMessageReader(stream)
	curr.mw = messageIO.NewMessageWriter(stream)
	nm.watching[p.ToString()] = curr
	nm.watchingLock.Unlock()
	switch stream := stream.(type) {
	case *net.TCPConn:
		go nm.handleTCPConnection(stream)
	case *net.UDPConn:
		go nm.handleUDPConnection(stream)
	}
	nm.logWatchingNodes()
	nm.startHbRoutine(p)
}

func (nm *NodeWatcherImpl) Watch(p peer.Peer, issuerProto protocol.ID) errors.Error {
	nm.logger.Infof("Proto %d request to watch %s", issuerProto, p.ToString())
	nm.watchingLock.Lock()
	if _, ok := nm.watching[p.ToString()]; ok {
		nm.watchingLock.Unlock()
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	nm.watching[p.ToString()] = NodeInfo{
		nrRetries:        0,
		closeOnce:        &sync.Once{},
		peer:             p,
		LatencyCalc:      analytics.NewLatencyCalculator(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight),
		Detector:         analytics.NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
		enoughSamples:    make(chan interface{}),
		nrMessagesNoWait: nm.conf.NrMessagesWithoutWait,
	}
	nm.watchingLock.Unlock()
	go nm.dialAndWatch(issuerProto, p)
	return nil
}

func (nm *NodeWatcherImpl) WatchWithInitialLatencyValue(p peer.Peer, issuerProto protocol.ID, latency time.Duration) errors.Error {
	nm.logger.Infof("Proto %d request to watch %s with initial value %s", issuerProto, p.ToString(), latency)
	nm.watchingLock.Lock()
	if _, ok := nm.watching[p.ToString()]; ok {
		nm.watchingLock.Unlock()
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	newNodeInfo := NodeInfo{
		nrRetries:        0,
		closeOnce:        &sync.Once{},
		peer:             p,
		LatencyCalc:      analytics.NewLatencyCalculatorWithValue(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight, latency),
		Detector:         analytics.NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
		enoughSamples:    make(chan interface{}),
		nrMessagesNoWait: 0,
	}
	newNodeInfo.LatencyCalc.AddMeasurement(latency)
	newNodeInfo.closeOnce.Do(func() {
		nm.closeEnoughSamplesChan(newNodeInfo)
	})
	nm.watching[p.ToString()] = newNodeInfo
	nm.watchingLock.Unlock()
	go nm.dialAndWatch(issuerProto, p)
	return nil
}

func (nm *NodeWatcherImpl) startHbRoutine(p peer.Peer) {
	ticker := time.NewTicker(nm.conf.HbTickDuration)
	for {
		<-ticker.C
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[p.ToString()]
		if !ok {
			nm.watchingLock.RUnlock()
			return
		}
		nm.watchingLock.RUnlock()
		toSend := analytics.NewHBMessageForceReply(nm.selfPeer)
		switch nodeInfo.peerConn.(type) {
		case net.PacketConn:
			//nm.logger.Infof("sending hb message:%+v", toSend)
			//nm.logger.Infof("Sent HB to %s via UDP", nodeInfo.peer.ToString())
			_, err := nodeInfo.peerConn.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		case net.Conn:
			//nm.logger.Infof("Sent HB to %s via TCP", nodeInfo.peer.ToString())
			_, err := nodeInfo.mw.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		}
	}
}

func (nm *NodeWatcherImpl) Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error {
	nm.logger.Infof("Proto %d request to unwatch %s", protoID, peer.ToString())
	nm.watchingLock.Lock()
	watchedPeer, ok := nm.watching[peer.ToString()]
	if !ok {
		nm.watchingLock.Unlock()
		nm.logger.Warn("peer not being tracked")
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	watchedPeer.peerConn.Close()
	delete(nm.watching, peer.ToString())
	nm.watchingLock.Unlock()
	nm.logWatchingNodes()
	return nil
}

func (nm *NodeWatcherImpl) GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error) {
	nm.watchingLock.RLock()
	defer nm.watchingLock.RUnlock()
	nodeInfo, ok := nm.watching[peer.ToString()]
	if !ok {
		return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	select {
	case <-nodeInfo.enoughSamples:
		return &nodeInfo, nil
	default:
		return nil, errors.NonFatalError(404, "Not enough samples", nodeWatcherCaller)
	}
}

func (nm *NodeWatcherImpl) GetNodeInfoWithDeadline(peer peer.Peer, deadline time.Time) (*NodeInfo, errors.Error) {
	nm.watchingLock.RLock()
	nodeInfo, ok := nm.watching[peer.ToString()]
	if !ok {
		nm.watchingLock.RUnlock()
		return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	nm.watchingLock.RUnlock()
	select {
	case <-nodeInfo.enoughSamples:
		nm.watchingLock.RLock()
		defer nm.watchingLock.RUnlock()
		nodeInfo, ok := nm.watching[peer.ToString()]
		if !ok {
			return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
		}
		return &nodeInfo, nil
	case <-time.After(time.Until(deadline)):
		return nil, errors.NonFatalError(404, "timed out waiting for enough samples", nodeWatcherCaller)
	}
}

func (nm *NodeWatcherImpl) Logger() *logrus.Logger {
	return nm.logger
}

func (nm *NodeWatcherImpl) establishStreamTo(p peer.Peer) (net.Conn, errors.Error) {

	//nm.logger.Infof("Establishing stream to %s", p.ToString())
	udpTestDeadline := time.Now().Add(nm.conf.UdpTestTimeout)
	rAddrUdp := &net.UDPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	udpConn, err := net.DialUDP("udp", nil, rAddrUdp)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}

	for i := 0; i < nm.conf.NrTestMessagesToSend; i++ {
		//nm.logger.Infof("Writing test message to: %s", p.ToString())
		_, err := udpConn.Write(analytics.SerializeHeartbeatMessage(analytics.NewHBMessageForceReply(nm.selfPeer)))
		if err != nil {
			break
		}
	}
	done := make(chan interface{})
	receivedMsgs := 0
	go func() {
		defer close(done)
		for receivedMsgs < nm.conf.NrTestMessagesToReceive {
			if err := udpConn.SetReadDeadline(udpTestDeadline); err != nil {
				nm.logger.Error(err)
				break
			}
			receiveBuf := make([]byte, 100)
			//nm.logger.Infof("Reading test message from: %s", p.ToString())
			_, _, err = udpConn.ReadFromUDP(receiveBuf)
			if err != nil {
				nm.logger.Warnf("Read error: %s", err.Error())
				break
			}
			receivedMsgs++
		}
		if err := udpConn.SetReadDeadline(time.Time{}); err != nil {
			nm.logger.Error(err)
		}

	}()
	<-done
	if receivedMsgs == nm.conf.NrTestMessagesToReceive {
		nm.logger.Infof("Established UDP connection to %s", p.ToString())
		return udpConn, nil
	}

	nm.logger.Warnf("falling back to TCP")
	// attempting to connect by TCP
	rAddrTcp := &net.TCPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	tcpConn, err := net.DialTimeout("tcp", rAddrTcp.String(), nm.conf.TcpTestTimeout)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}
	return tcpConn, nil
}

func (nm *NodeWatcherImpl) start() {
	go nm.startTCPServer()
	go nm.startUDPServer()
	go nm.evalCondsPeriodic()
}

func (nm *NodeWatcherImpl) attemptRepairStreamTo(p peer.Peer) {
	stream, err := nm.establishStreamTo(p)
	nm.watchingLock.Lock()
	defer nm.watchingLock.Unlock()
	currStatus := nm.watching[p.ToString()]
	if err != nil {
		currStatus.nrRetries++
		if currStatus.nrRetries >= nm.conf.MaxRedials {
			nm.logger.Warnf("Peer %s removed because it has exceeded max re-dials (%d/%d)", p.ToString(), currStatus.nrRetries, nm.conf.MaxRedials)
			delete(nm.watching, p.ToString())
			return
		}
		nm.watching[p.ToString()] = currStatus
		return
	}
	currStatus.peerConn = stream
	currStatus.mr = messageIO.NewMessageReader(stream)
	currStatus.mw = messageIO.NewMessageWriter(stream)
	nm.watching[p.ToString()] = currStatus
}

func (nm *NodeWatcherImpl) startUDPServer() {
	listenAddr := &net.UDPAddr{
		IP:   nm.selfPeer.IP(),
		Port: int(nm.selfPeer.AnalyticsPort()),
	}

	udpConn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		panic(err)
	}

	for {
		msgBuf := make([]byte, 2048)
		n, rAddr, rErr := udpConn.ReadFromUDP(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			continue
		}
		go nm.handleHBMessageUDP(msgBuf[:n], udpConn, rAddr)
	}
}

func (nm *NodeWatcherImpl) startTCPServer() {
	listenAddr := &net.TCPAddr{
		IP:   nm.selfPeer.IP(),
		Port: int(nm.selfPeer.AnalyticsPort()),
	}

	listener, err := net.ListenTCP("tcp", listenAddr)
	if err != nil {
		panic(err)
	}

	for {
		newStream, err := listener.AcceptTCP()
		if err != nil {
			panic(err)
		}
		nm.logger.Infof("New tcp conn established")
		go nm.handleTCPConnection(newStream)
	}
}

func (nm *NodeWatcherImpl) handleTCPConnection(inConn *net.TCPConn) {
	mr := messageIO.NewMessageReader(inConn)
	mw := messageIO.NewMessageWriter(inConn)
	for {
		msgBuf := make([]byte, 2048)
		n, rErr := mr.Read(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			return
		}
		go nm.handleHBMessage(msgBuf[:n], mw)
	}
}

func (nm *NodeWatcherImpl) handleUDPConnection(inConn *net.UDPConn) {
	for {
		msgBuf := make([]byte, 2048)
		n, rErr := inConn.Read(msgBuf)
		if rErr != nil {
			nm.logger.Warn("handleUDPConnection exiting")
			nm.logger.Warn(rErr)
			return
		}
		go nm.handleHBMessage(msgBuf[:n], inConn)
	}
}

func (nm *NodeWatcherImpl) handleHBMessage(hbBytes []byte, mw io.Writer) errors.Error {
	hb := analytics.DeserializeHeartbeatMessage(hbBytes)

	//nm.logger.Infof("handling hb message:%+v", hb)
	if hb.IsReply {
		sender := hb.Sender
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[sender.ToString()]
		if !ok {
			nm.watchingLock.RUnlock()
			nm.logger.Warn("Received reply for unwatched node, discarding...")
			return errors.NonFatalError(500, "Received reply for unwatched node, discarding...", nodeWatcherCaller)
		}
		nm.watchingLock.RUnlock()
		//nm.logger.Infof("handling hb reply message")
		if nodeInfo.LatencyCalc.NrMeasurements() < nodeInfo.nrMessagesNoWait && nodeInfo.nrMessagesNoWait != 0 {
			timeTaken := time.Since(hb.TimeStamp)
			nodeInfo.LatencyCalc.AddMeasurement(timeTaken)
			if nodeInfo.LatencyCalc.NrMeasurements() >= nm.conf.MinSamplesLatencyEstimate {
				nodeInfo.closeOnce.Do(func() {
					nm.closeEnoughSamplesChan(nodeInfo)
				})
			}
			nm.logger.Infof("Sending fast message nr %d to peer %s", nodeInfo.LatencyCalc.NrMeasurements(), sender.ToString())
			toSend := analytics.NewHBMessageForceReply(nm.selfPeer)
			_, err := mw.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				return errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
			}
			return nil
		}
		nm.registerHBReply(hb, nodeInfo)
		return nil
	}
	if hb.ForceReply {
		//nm.logger.Infof("replying to hb message")
		toSend := analytics.HeartbeatMessage{
			TimeStamp:  hb.TimeStamp,
			Sender:     nm.selfPeer,
			IsReply:    true,
			ForceReply: false,
		}
		_, err := mw.Write(analytics.SerializeHeartbeatMessage(toSend))
		if err != nil {
			return errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
		}
		return nil
	}
	panic("Should not be here")
}

func (nm *NodeWatcherImpl) handleHBMessageUDP(hbBytes []byte, udpConn *net.UDPConn, rAddr *net.UDPAddr) errors.Error {
	hb := analytics.DeserializeHeartbeatMessage(hbBytes)

	//nm.logger.Infof("handling hb message:%+v", hb)
	if hb.IsReply {
		sender := hb.Sender
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[sender.ToString()]
		if !ok {
			nm.watchingLock.RUnlock()
			nm.logger.Warn("Received reply for unwatched node, discarding...")
			return errors.NonFatalError(500, "Received reply for unwatched node, discarding...", nodeWatcherCaller)
		}
		nm.watchingLock.RUnlock()
		//nm.logger.Infof("handling hb reply message")
		if nodeInfo.LatencyCalc.NrMeasurements() < nodeInfo.nrMessagesNoWait && nodeInfo.nrMessagesNoWait != 0 {
			toSend := analytics.NewHBMessageForceReply(nm.selfPeer)
			_, err := udpConn.WriteTo(analytics.SerializeHeartbeatMessage(toSend), rAddr)
			if err != nil {
				return errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
			}
			timeTaken := time.Since(hb.TimeStamp)
			nodeInfo.LatencyCalc.AddMeasurement(timeTaken)
			if nodeInfo.LatencyCalc.NrMeasurements() >= nm.conf.MinSamplesLatencyEstimate {
				nodeInfo.closeOnce.Do(func() {
					nm.closeEnoughSamplesChan(nodeInfo)
				})
			}
			return nil
		}
		nm.registerHBReply(hb, nodeInfo)
		return nil
	}
	if hb.ForceReply {
		//nm.logger.Infof("replying to hb message")
		toSend := analytics.HeartbeatMessage{
			TimeStamp:  hb.TimeStamp,
			Sender:     nm.selfPeer,
			IsReply:    true,
			ForceReply: false,
		}
		_, err := udpConn.WriteTo(analytics.SerializeHeartbeatMessage(toSend), rAddr)
		if err != nil {
			return errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
		}
		return nil
	}
	panic("Should not be here")
}

func (nm *NodeWatcherImpl) registerHBReply(hb analytics.HeartbeatMessage, nodeInfo NodeInfo) {
	timeReceived := time.Now()
	timeTaken := time.Since(hb.TimeStamp)
	nodeInfo.Detector.Ping(timeReceived)
	nodeInfo.LatencyCalc.AddMeasurement(timeTaken)
	if nodeInfo.LatencyCalc.NrMeasurements() >= nm.conf.MinSamplesLatencyEstimate {
		nodeInfo.closeOnce.Do(func() {
			nm.closeEnoughSamplesChan(nodeInfo)
		})
	}
}

func (nm *NodeWatcherImpl) closeEnoughSamplesChan(nodeInfo NodeInfo) {
	nm.logger.Infof("Closing enoughSamples chan for peer %s", nodeInfo.peer.ToString())
	lat := nodeInfo.LatencyCalc.CurrValue()
	nm.logger.Infof("Node %s: ConnType: %s PHI: %f, Latency: %d, Subscribers: %d ", nodeInfo.peer.ToString(), reflect.TypeOf(nodeInfo.peerConn), nodeInfo.Detector.Phi(time.Now()), lat, len(nodeInfo.subscribers))
	close(nodeInfo.enoughSamples)
}
func (nm *NodeWatcherImpl) logWatchingNodes() {
	nm.watchingLock.RLock()
	nm.logger.Infof("Watching peers:")
	for _, p := range nm.watching {
		nm.logger.Infof("%s", p.peer.ToString())
	}
	nm.watchingLock.RUnlock()
}

func (nm *NodeWatcherImpl) CancelCond(condID int) errors.Error {
	responseChan := make(chan int)
	defer close(responseChan)
	nm.cancelCondChan <- &cancelCondReq{key: condID, removed: responseChan}
	response := <-responseChan
	if response == -1 {
		return errors.NonFatalError(404, "condition not found", nodeWatcherCaller)
	}
	return nil
}

func (nm *NodeWatcherImpl) NotifyOnCondition(c Condition) (int, errors.Error) {
	condId := rand.Int()
	conditionsItem := &dataStructures.Item{
		Value:    c,
		Key:      condId,
		Priority: time.Now().Add(c.EvalConditionTickDuration).Unix(),
	}
	nm.addCondChan <- conditionsItem
	return condId, nil
}

func (nm *NodeWatcherImpl) evalCondsPeriodic() {
	var waitTime time.Duration
	var nextItem *dataStructures.Item

LOOP:
	for {
		var nextItemTimer = time.NewTimer(math.MaxInt64)
		if nm.conditions.Len() > 0 {
			nextItem = heap.Pop(&nm.conditions).(*dataStructures.Item)
			waitTime = time.Until(time.Unix(0, nextItem.Priority))
			nextItemTimer = time.NewTimer(waitTime)
		}
		select {
		case newItem := <-nm.addCondChan:
			nm.logger.Infof("Received add condition signal...")

			nm.logger.Infof("Adding condition %d", newItem.Key)
			heap.Push(&nm.conditions, newItem)

			if nextItem != nil {
				nm.logger.Infof("nextItem (%d) was not nil, re-adding to condition list", nextItem.Key)
				heap.Push(&nm.conditions, nextItem)
			}
			nm.conditions.LogEntries(nm.logger)
		case req := <-nm.cancelCondChan:
			nm.logger.Infof("Received cancel cond signal...")
			if req.key == nextItem.Key {
				req.removed <- req.key
				nm.logger.Infof("Removed condition %d successfully", req.key)
				continue LOOP
			}

			heap.Push(&nm.conditions, nextItem)
			aux := nm.removeCond(req.key)
			if aux != -1 {
				nm.logger.Infof("Removed condition %d successfully", req.key)
			} else {
				nm.logger.Warnf("Removing condition %d failure: not found", req.key)
			}
			req.removed <- aux
			nm.conditions.LogEntries(nm.logger)
		case <-nextItemTimer.C:
			nm.logger.Infof("Processing condition %+v", *nextItem)
			cond := nextItem.Value.(Condition)
			nm.watchingLock.RLock()
			nodeStats := nm.watching[cond.Peer.ToString()]
			nm.watchingLock.RUnlock()
			if cond.CondFunc(nodeStats) {
				SendNotification(cond.Notification)
				if cond.Repeatable {
					if cond.EnableGracePeriod {
						nextItem.Priority = time.Now().Add(cond.GracePeriod).UnixNano()
						heap.Push(&nm.conditions, nextItem)
						continue
					}
					nextItem.Priority = time.Now().Add(cond.EvalConditionTickDuration).UnixNano()
					heap.Push(&nm.conditions, nextItem)
				}
			}
			nm.conditions.LogEntries(nm.logger)
		}
	}
}

func (nm *NodeWatcherImpl) removeCond(condId int) int {
	nm.logger.Infof("Canceling timer with ID %d", condId)
	for idx, entry := range nm.conditions {
		if entry.Key == condId {
			heap.Remove(&nm.conditions, idx)
			return entry.Key
		}
	}
	heap.Init(&nm.conditions)
	return -1
}
