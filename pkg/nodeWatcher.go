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
	"go.uber.org/atomic"
)

const nodeWatcherCaller = "NodeWatcher"

type ConditionFunc = func(NodeInfo) bool

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
	mw       messageIO.MessageWriter
	mr       messageIO.MessageReader
	peerConn net.Conn

	nrMessagesReceived *atomic.Int32
	nrRetries          int
	subscribers        map[protocol.ID]bool
	peer               peer.Peer
	closeOnce          *sync.Once
	nrMessagesNoWait   int
	enoughSamples      chan interface{}
	LatencyCalc        *analytics.LatencyCalculator
	Detector           *analytics.Detector
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
	udpConn      net.UDPConn

	conditions     dataStructures.PriorityQueue
	addCondChan    chan *dataStructures.Item
	cancelCondChan chan *cancelCondReq

	logger *logrus.Logger
}

type NodeWatcherConf struct {
	PrintLatencyToInterval    time.Duration
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
		conf:           config,
		selfPeer:       selfPeer,
		watching:       make(map[string]NodeInfo),
		watchingLock:   &sync.RWMutex{},
		conditions:     make(dataStructures.PriorityQueue, 0),
		logger:         logs.NewLogger(nodeWatcherCaller),
		addCondChan:    make(chan *dataStructures.Item),
		cancelCondChan: make(chan *cancelCondReq),
	}

	if nm.conf.OldLatencyWeight+nm.conf.NewLatencyWeight != 1 {
		panic("OldLatencyWeight + NewLatencyWeight != 1")
	}

	listenAddr := &net.UDPAddr{
		IP:   nm.selfPeer.IP(),
		Port: int(nm.selfPeer.AnalyticsPort()),
	}

	udpConn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		panic(err)
	}
	nm.udpConn = *udpConn
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
	curr := nm.watching[p.String()]
	curr.peerConn = stream
	switch stream := stream.(type) {
	case *net.TCPConn:
		curr.mr = messageIO.NewMessageReader(stream)
		curr.mw = messageIO.NewMessageWriter(stream)
		go nm.handleTCPConnection(curr.mr, curr.mw)
	}
	nm.watching[p.String()] = curr
	nm.watchingLock.Unlock()
	nm.logWatchingNodes()
	nm.startHbRoutine(p)
}

func (nm *NodeWatcherImpl) Watch(p peer.Peer, issuerProto protocol.ID) errors.Error {
	if p == nil {
		panic("Tried to watch nil peer")
	}
	nm.logger.Infof("Proto %d request to watch %s", issuerProto, p.String())
	nm.watchingLock.Lock()
	if _, ok := nm.watching[p.String()]; ok {
		nm.watchingLock.Unlock()
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	nm.watching[p.String()] = NodeInfo{
		nrRetries:          0,
		closeOnce:          &sync.Once{},
		nrMessagesReceived: &atomic.Int32{},
		peer:               p,
		LatencyCalc:        analytics.NewLatencyCalculator(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight),
		Detector:           analytics.NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
		enoughSamples:      make(chan interface{}),
	}
	nm.watchingLock.Unlock()
	go nm.dialAndWatch(issuerProto, p)
	return nil
}

func (nm *NodeWatcherImpl) WatchWithInitialLatencyValue(p peer.Peer, issuerProto protocol.ID, latency time.Duration) errors.Error {
	nm.logger.Infof("Proto %d request to watch %s with initial value %s", issuerProto, p.String(), latency)
	if p == nil {
		panic("Tried to watch nil peer")
	}
	nm.watchingLock.Lock()
	if _, ok := nm.watching[p.String()]; ok {
		nm.watchingLock.Unlock()
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	newNodeInfo := NodeInfo{
		nrRetries:          0,
		closeOnce:          &sync.Once{},
		nrMessagesReceived: &atomic.Int32{},
		peer:               p,
		LatencyCalc:        analytics.NewLatencyCalculatorWithValue(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight, latency),
		Detector:           analytics.NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
		enoughSamples:      make(chan interface{}),
		nrMessagesNoWait:   0,
	}
	newNodeInfo.LatencyCalc.AddMeasurement(latency)
	newNodeInfo.closeOnce.Do(func() {
		nm.closeEnoughSamplesChan(newNodeInfo)
	})
	nm.watching[p.String()] = newNodeInfo
	nm.watchingLock.Unlock()
	go nm.dialAndWatch(issuerProto, p)
	return nil
}

func (nm *NodeWatcherImpl) startHbRoutine(p peer.Peer) {

	rAddr := &net.UDPAddr{
		IP:   p.IP(),
		Port: int(p.AnalyticsPort()),
	}

	nm.watchingLock.RLock()
	nodeInfo, ok := nm.watching[p.String()]
	if !ok {
		nm.watchingLock.RUnlock()
		return
	}
	nm.watchingLock.RUnlock()

	toSend := analytics.NewHBMessageForceReply(nm.selfPeer, false)
	switch nodeInfo.peerConn.(type) {
	case net.PacketConn:
		for i := 0; i < nm.conf.NrMessagesWithoutWait; i++ {
			_, err := nm.udpConn.WriteTo(analytics.SerializeHeartbeatMessage(toSend), rAddr)
			if err != nil {
				panic("err in udp conn")
			}
		}
	case net.Conn:
		for i := 0; i < nm.conf.NrMessagesWithoutWait; i++ {
			_, err := nodeInfo.mw.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				return
			}
		}
	}

	ticker := time.NewTicker(nm.conf.HbTickDuration)
	for {
		<-ticker.C
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[p.String()]
		if !ok {
			nm.watchingLock.RUnlock()
			return
		}
		nm.watchingLock.RUnlock()

		toSend := analytics.NewHBMessageForceReply(nm.selfPeer, false)
		switch nodeInfo.peerConn.(type) {
		case net.PacketConn:
			//nm.logger.Infof("sending hb message:%+v", toSend)
			//nm.logger.Infof("Sent HB to %s via UDP", nodeInfo.peer.ToString())
			_, err := nm.udpConn.WriteTo(analytics.SerializeHeartbeatMessage(toSend), rAddr)
			if err != nil {
				panic("err in udp conn")
			}

		case net.Conn:
			//nm.logger.Infof("Sent HB to %s via TCP", nodeInfo.peer.ToString())
			_, err := nodeInfo.mw.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				return
			}
		}
	}
}

func (nm *NodeWatcherImpl) Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error {
	nm.logger.Infof("Proto %d request to unwatch %s", protoID, peer.String())
	nm.watchingLock.Lock()
	watchedPeer, ok := nm.watching[peer.String()]
	if !ok {
		nm.watchingLock.Unlock()
		nm.logger.Warn("peer not being tracked")
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	delete(nm.watching, peer.String())
	_, ok = watchedPeer.peerConn.(*net.TCPConn)
	if ok {
		watchedPeer.mr.Close()
	}
	nm.watchingLock.Unlock()
	nm.logWatchingNodes()
	return nil
}

func (nm *NodeWatcherImpl) GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error) {
	nm.watchingLock.RLock()
	defer nm.watchingLock.RUnlock()
	nodeInfo, ok := nm.watching[peer.String()]
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
	nodeInfo, ok := nm.watching[peer.String()]
	if !ok {
		nm.watchingLock.RUnlock()
		return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	nm.watchingLock.RUnlock()
	select {
	case <-nodeInfo.enoughSamples:
		nm.watchingLock.RLock()
		defer nm.watchingLock.RUnlock()
		nodeInfo, ok := nm.watching[peer.String()]
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

	for i := 0; i < nm.conf.MaxRedials; i++ {
		for i := 0; i < nm.conf.NrTestMessagesToSend; i++ {
			//nm.logger.Infof("Writing test message to: %s", p.ToString())
			_, err := nm.udpConn.WriteToUDP(analytics.SerializeHeartbeatMessage(analytics.NewHBMessageForceReply(nm.selfPeer, true)), rAddrUdp)
			if err != nil {
				panic(err)
			}
		}
		<-time.After(time.Until(udpTestDeadline))

		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[p.String()]
		if !ok {
			nm.watchingLock.RUnlock()
			return nil, errors.NonFatalError(500, "peer unwatched in the meantime", nodeWatcherCaller)
		}

		if int(nodeInfo.nrMessagesReceived.Load()) >= nm.conf.NrTestMessagesToReceive {
			nm.watchingLock.RUnlock()
			nm.logger.Infof("Established UDP connection to %s", p.String())
			return &nm.udpConn, nil
		}
		nm.watchingLock.RUnlock()

		nm.logger.Warnf("falling back to TCP")
		// attempting to connect by TCP
		rAddrTcp := &net.TCPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
		tcpConn, err := net.DialTimeout("tcp", rAddrTcp.String(), nm.conf.TcpTestTimeout)
		if err != nil {
			continue
		}
		return tcpConn, nil
	}
	return nil, errors.NonFatalError(500, "Could not establish connection", nodeWatcherCaller)
}

func (nm *NodeWatcherImpl) start() {
	go nm.startTCPServer()
	go nm.startUDPServer()
	go nm.evalCondsPeriodic()
	go nm.printLatencyToPeriodic()
}

func (nm *NodeWatcherImpl) startUDPServer() {
	for {
		msgBuf := make([]byte, 2048)
		n, _, rErr := nm.udpConn.ReadFromUDP(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			continue
		}
		go nm.handleHBMessageUDP(msgBuf[:n])
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
		mr := messageIO.NewMessageReader(newStream)
		mw := messageIO.NewMessageWriter(newStream)
		go nm.handleTCPConnection(mr, mw)
	}
}

func (nm *NodeWatcherImpl) handleTCPConnection(mr messageIO.MessageReader, mw messageIO.MessageWriter) {
	defer mr.Close()
	for {
		msgBuf := make([]byte, 2048)
		n, rErr := mr.Read(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			return
		}
		err := nm.handleHBMessageTCP(msgBuf[:n], mw)
		if err != nil {
			nm.logger.Warn(err.Reason())
			return
		}
	}
}
func (nm *NodeWatcherImpl) handleHBMessageTCP(hbBytes []byte, mw io.Writer) errors.Error {
	hb := analytics.DeserializeHeartbeatMessage(hbBytes)
	nm.logger.Infof("handling hb message via TCP :%+v", hb)
	if hb.IsReply {
		sender := hb.Sender
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[sender.String()]
		if !ok {
			nm.watchingLock.RUnlock()
			nm.logger.Warn("Received reply for unwatched node, discarding...")
			return errors.NonFatalError(500, "Received reply for unwatched node, discarding...", nodeWatcherCaller)
		}
		nm.watchingLock.RUnlock()
		//nm.logger.Infof("handling hb reply message")
		nm.registerHBReply(hb, nodeInfo)
		return nil
	}
	if hb.ForceReply {

		//nm.logger.Infof("replying to hb message")
		toSend := analytics.HeartbeatMessage{
			TimeStamp:  hb.TimeStamp,
			Initial:    hb.Initial,
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

func (nm *NodeWatcherImpl) handleHBMessageUDP(hbBytes []byte) errors.Error {
	hb := analytics.DeserializeHeartbeatMessage(hbBytes)

	if hb.IsReply {
		sender := hb.Sender
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[sender.String()]
		if !ok {
			nm.watchingLock.RUnlock()
			nm.logger.Warn("Received reply for unwatched node, discarding...")
			return errors.NonFatalError(500, "Received reply for unwatched node, discarding...", nodeWatcherCaller)
		}
		nm.watchingLock.RUnlock()
		nm.registerHBReply(hb, nodeInfo)
		return nil
	}
	if hb.ForceReply {
		//nm.logger.Infof("replying to hb message")
		toSend := analytics.HeartbeatMessage{
			TimeStamp:  hb.TimeStamp,
			Initial:    hb.Initial,
			Sender:     nm.selfPeer,
			IsReply:    true,
			ForceReply: false,
		}

		rAddr := net.UDPAddr{
			IP:   hb.Sender.IP(),
			Port: int(hb.Sender.AnalyticsPort()),
		}

		_, err := nm.udpConn.WriteToUDP(analytics.SerializeHeartbeatMessage(toSend), &rAddr)
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
	nodeInfo.LatencyCalc.AddMeasurement(timeTaken)
	if nodeInfo.LatencyCalc.NrMeasurements() >= nm.conf.MinSamplesLatencyEstimate {
		nodeInfo.closeOnce.Do(func() {
			nm.closeEnoughSamplesChan(nodeInfo)
		})
	}
	if !hb.Initial {
		nodeInfo.Detector.Ping(timeReceived)
	}
	nodeInfo.nrMessagesReceived.Add(1)
}

func (nm *NodeWatcherImpl) closeEnoughSamplesChan(nodeInfo NodeInfo) {
	nm.logger.Infof("Closing enoughSamples chan for peer %s", nodeInfo.peer.String())
	lat := nodeInfo.LatencyCalc.CurrValue()
	nm.logger.Infof("Node %s: ConnType: %s PHI: %f, Latency: %d, Subscribers: %d ", nodeInfo.peer.String(), reflect.TypeOf(nodeInfo.peerConn), nodeInfo.Detector.Phi(time.Now()), lat, len(nodeInfo.subscribers))
	close(nodeInfo.enoughSamples)
}
func (nm *NodeWatcherImpl) logWatchingNodes() {
	nm.watchingLock.RLock()
	toPrint := ""
	for _, watchedNode := range nm.watching {
		toPrint = toPrint + "; " + watchedNode.peer.String()
	}
	nm.logger.Infof("Watching peers: %s:", toPrint)
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
	if c.Peer == nil {
		nm.logger.Panic("peer to notify is nil")
	}
	conditionsItem := &dataStructures.Item{
		Value:    c,
		Key:      condId,
		Priority: time.Now().Add(c.EvalConditionTickDuration).Unix(),
	}
	nm.logger.Infof("Adding condition %+v for peer %s", c, c.Peer.String())
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
			//nm.conditions.LogEntries(nm.logger)
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
			//nm.conditions.LogEntries(nm.logger)
		case <-nextItemTimer.C:
			nm.logger.Infof("Condition trigger: %+v", *nextItem)
			cond := nextItem.Value.(Condition)
			nm.watchingLock.RLock()
			nodeStats := nm.watching[cond.Peer.String()]
			nm.watchingLock.RUnlock()
			select {
			case <-nodeStats.enoughSamples:
				if cond.CondFunc(nodeStats) {
					nm.logger.Infof("Condition trigger: %+v", *nextItem)
					SendNotification(cond.Notification)
					if cond.Repeatable {
						if cond.EnableGracePeriod {
							nextItem.Priority = time.Now().Add(cond.GracePeriod).UnixNano()
							heap.Push(&nm.conditions, nextItem)
							continue LOOP
						}
						nextItem.Priority = time.Now().Add(cond.EvalConditionTickDuration).UnixNano()
						heap.Push(&nm.conditions, nextItem)
						continue LOOP
					}
				}
				//nm.conditions.LogEntries(nm.logger)
			default:
				nextItem.Priority = time.Now().Add(cond.EvalConditionTickDuration).UnixNano()
				heap.Push(&nm.conditions, nextItem)
			}
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

func (nm *NodeWatcherImpl) printLatencyToPeriodic() {
	if nm.conf.PrintLatencyToInterval == 0 {
		return
	}
	ticker := time.NewTicker(nm.conf.PrintLatencyToInterval)
	for {
		<-ticker.C
		nm.watchingLock.RLock()
		for _, watchedPeer := range nm.watching {
			select {
			case <-watchedPeer.enoughSamples:
				nm.logger.Infof("Node %s: ConnType: %s PHI: %f, Latency: %d, Subscribers: %d ", watchedPeer.peer.String(), reflect.TypeOf(watchedPeer.peerConn), watchedPeer.Detector.Phi(time.Now()), watchedPeer.LatencyCalc.CurrValue(), len(watchedPeer.subscribers))
			default:
			}
		}
		nm.watchingLock.RUnlock()
	}

}
