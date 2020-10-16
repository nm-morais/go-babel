package pkg

import (
	"container/heap"
	"math"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/analytics"
	priorityqueue "github.com/nm-morais/go-babel/pkg/dataStructures/priorityQueue"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/peer"
	. "github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/sirupsen/logrus"
)

const nodeWatcherCaller = "NodeWatcher"

type NodeInfoImpl struct {
	peerConn           net.Conn
	nrMessagesReceived *int32
	subscribers        map[protocol.ID]bool
	nrMessagesNoWait   int

	enoughTestMessages chan interface{}
	enoughSamples      chan interface{}
	err                chan interface{}
	unwatch            chan interface{}

	peer        peer.Peer
	latencyCalc *analytics.LatencyCalculator
	detector    *analytics.PhiAccuralFailureDetector
}

func (n *NodeInfoImpl) Peer() peer.Peer {
	return n.peer
}

func (n *NodeInfoImpl) LatencyCalc() *analytics.LatencyCalculator {
	return n.latencyCalc
}

func (n *NodeInfoImpl) Detector() *analytics.PhiAccuralFailureDetector {
	return n.detector
}

type nodeWatchingValue = *NodeInfoImpl

type cancelCondReq struct {
	key     int
	removed chan int
}

type NodeWatcherImpl struct {
	babel protocolManager.ProtocolManager

	selfPeer peer.Peer
	conf     NodeWatcherConf

	watching sync.Map

	udpConn net.UDPConn

	conditions          priorityqueue.PriorityQueue
	addCondChan         chan *priorityqueue.Item
	cancelCondChan      chan *cancelCondReq
	removeNodeCondsChan chan peer.Peer

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
	MinSamplesLatencyEstimate int
	OldLatencyWeight          float32
	NewLatencyWeight          float32

	PhiThreshold           float64
	WindowSize             int
	MinStdDeviation        time.Duration
	AcceptableHbPause      time.Duration
	FirstHeartbeatEstimate time.Duration
}

// threshold float64,
// 	maxSampleSize uint,
// 	minStdDeviation time.Duration,
// 	acceptableHeartbeatPause time.Duration,
// 	firstHeartbeatEstimate time.Duration,
// 	eventStream chan<- time.Duration) (*PhiAccuralFailureDetector, error) {

// var streamConfig :=

func NewNodeWatcher(config NodeWatcherConf, babel protocolManager.ProtocolManager) *NodeWatcherImpl {
	nm := &NodeWatcherImpl{
		babel:               babel,
		conf:                config,
		selfPeer:            babel.SelfPeer(),
		watching:            sync.Map{},
		conditions:          make(priorityqueue.PriorityQueue, 0),
		logger:              logs.NewLogger(nodeWatcherCaller),
		addCondChan:         make(chan *priorityqueue.Item),
		cancelCondChan:      make(chan *cancelCondReq),
		removeNodeCondsChan: make(chan peer.Peer),
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

func (nm *NodeWatcherImpl) dialAndWatch(issuerProto protocol.ID, nodeInfo *NodeInfoImpl) {
	stream, err := nm.establishStreamTo(nodeInfo.Peer(), nodeInfo)
	if err != nil {
		close(nodeInfo.err)
		return
	}
	nodeInfo.peerConn = stream
	switch stream := stream.(type) {
	case *net.TCPConn:
		go nm.handleTCPConnection(stream)
	}
	nm.startHbRoutine(nodeInfo)
}

func (nm *NodeWatcherImpl) Watch(p peer.Peer, issuerProto protocol.ID) errors.Error {
	if reflect.ValueOf(p).IsNil() {
		panic("Tried to watch nil peer")
	}
	nm.logger.Infof("Proto %d request to watch %s", issuerProto, p.String())
	if _, ok := nm.watching.Load(p.String()); ok {
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	var nrMessages int32 = 0
	detector, err := analytics.New(nm.conf.PhiThreshold, uint(nm.conf.WindowSize), nm.conf.MinStdDeviation, nm.conf.AcceptableHbPause, nm.conf.FirstHeartbeatEstimate, nil)
	if err != nil {
		panic(err)
	}
	nodeInfo := &NodeInfoImpl{
		nrMessagesReceived: &nrMessages,
		peer:               p,
		latencyCalc:        analytics.NewLatencyCalculator(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight),
		detector:           detector,
		enoughSamples:      make(chan interface{}),
		enoughTestMessages: make(chan interface{}),
		err:                make(chan interface{}),
		unwatch:            make(chan interface{}),
	}
	nm.watching.Store(p.String(), nodeInfo)
	go nm.dialAndWatch(issuerProto, nodeInfo)
	return nil
}

func (nm *NodeWatcherImpl) WatchWithInitialLatencyValue(p peer.Peer, issuerProto protocol.ID, latency time.Duration) errors.Error {
	nm.logger.Infof("Proto %d request to watch %s with initial value %s", issuerProto, p.String(), latency)
	if reflect.ValueOf(p).IsNil() {
		panic("Tried to watch nil peer")
	}
	if _, ok := nm.watching.Load(p.String()); ok {
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	var nrMessages int32 = 0
	detector, err := analytics.New(nm.conf.PhiThreshold, uint(nm.conf.WindowSize), nm.conf.MinStdDeviation, nm.conf.AcceptableHbPause, nm.conf.FirstHeartbeatEstimate, nil)
	if err != nil {
		panic(err)
	}
	newNodeInfo := &NodeInfoImpl{
		nrMessagesReceived: &nrMessages,
		peer:               p,
		latencyCalc:        analytics.NewLatencyCalculatorWithValue(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight, latency),
		detector:           detector,
		enoughSamples:      make(chan interface{}),
		enoughTestMessages: make(chan interface{}),
		err:                make(chan interface{}),
		unwatch:            make(chan interface{}),
		nrMessagesNoWait:   0,
	}
	close(newNodeInfo.enoughSamples)
	close(newNodeInfo.enoughTestMessages)
	newNodeInfo.latencyCalc.AddMeasurement(latency)
	nm.watching.Store(p.String(), newNodeInfo)
	go nm.dialAndWatch(issuerProto, newNodeInfo)
	return nil
}

func (nm *NodeWatcherImpl) startHbRoutine(nodeInfo *NodeInfoImpl) {
	toSend := analytics.NewHBMessageForceReply(nm.selfPeer, true)
	ticker := time.NewTicker(nm.conf.HbTickDuration)
	switch conn := nodeInfo.peerConn.(type) {
	case net.PacketConn:
		rAddrUdp := &net.UDPAddr{IP: nodeInfo.Peer().IP(), Port: int(nodeInfo.Peer().AnalyticsPort())}

		for i := 0; i < nm.conf.NrMessagesWithoutWait; i++ {
			_, err := nm.udpConn.WriteToUDP(analytics.SerializeHeartbeatMessage(toSend), rAddrUdp)
			if err != nil {
				panic("err in udp conn")
			}
		}
		for {
			select {
			case <-ticker.C:
				toSend := analytics.NewHBMessageForceReply(nm.selfPeer, false)
				//nm.logger.Infof("sending hb message:%+v", toSend)
				//nm.logger.Infof("Sent HB to %s via UDP", nodeInfo.*peer.ToString())
				_, err := nm.udpConn.WriteToUDP(analytics.SerializeHeartbeatMessage(toSend), rAddrUdp)
				if err != nil {
					close(nodeInfo.err)
					return
				}
			case <-nodeInfo.unwatch:
				return
			}
		}
	case net.Conn:
		frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, conn)
		for i := 0; i < nm.conf.NrMessagesWithoutWait; i++ {
			err := frameBasedConn.WriteFrame(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				close(nodeInfo.err)
				return
			}
		}
		for {
			select {
			case <-ticker.C:
				toSend := analytics.NewHBMessageForceReply(nm.selfPeer, false)
				err := frameBasedConn.WriteFrame(analytics.SerializeHeartbeatMessage(toSend))
				if err != nil {
					close(nodeInfo.err)
					return
				}
			case <-nodeInfo.unwatch:
				return
			}

		}
	}
}

func (nm *NodeWatcherImpl) Unwatch(peer Peer, protoID protocol.ID) errors.Error {
	nm.logger.Infof("Proto %d request to unwatch %s", protoID, peer.String())
	watchedPeer, ok := nm.watching.Load(peer.String())
	if !ok {
		nm.logger.Warn("peer not being tracked")
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	close(watchedPeer.(nodeWatchingValue).unwatch)
	nm.removeNodeCondsChan <- peer
	nm.watching.Delete(peer.String())
	return nil
}

func (nm *NodeWatcherImpl) GetNodeInfo(peer peer.Peer) (nodeWatcher.NodeInfo, errors.Error) {
	nodeInfoInt, ok := nm.watching.Load(peer.String())
	if !ok {
		return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	nodeInfo := nodeInfoInt.(nodeWatchingValue)
	select {
	case <-nodeInfo.enoughSamples:
		return nodeInfo, nil
	case <-nodeInfo.err:
		return nil, errors.NonFatalError(404, "An error ocurred watching node %s", peer.String())
	case <-nodeInfo.unwatch:
		return nil, errors.NonFatalError(404, "An error ocurred watching node %s", peer.String())
	default:
		return nil, errors.NonFatalError(404, "Not enough samples", nodeWatcherCaller)
	}
}

func (nm *NodeWatcherImpl) GetNodeInfoWithDeadline(peer peer.Peer, deadline time.Time) (nodeWatcher.NodeInfo, errors.Error) {
	nodeInfoInt, ok := nm.watching.Load(peer.String())
	if !ok {
		return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	nodeInfo := nodeInfoInt.(nodeWatchingValue)
	select {
	case <-nodeInfo.err:
		return nil, errors.NonFatalError(404, "An error ocurred watching node %s", peer.String())
	case <-nodeInfo.unwatch:
		return nil, errors.NonFatalError(404, "An error ocurred watching node %s", peer.String())
	case <-nodeInfo.enoughSamples:
		return nodeInfo, nil
	case <-time.After(time.Until(deadline)):
		return nil, errors.NonFatalError(404, "timed out waiting for enough samples", nodeWatcherCaller)
	}
}

func (nm *NodeWatcherImpl) Logger() *logrus.Logger {
	return nm.logger
}

func (nm *NodeWatcherImpl) establishStreamTo(p peer.Peer, nodeInfo *NodeInfoImpl) (net.Conn, errors.Error) {
	//nm.logger.Infof("Establishing stream to %s", p.ToString())

	udpTestDeadline := time.Now().Add(nm.conf.UdpTestTimeout)
	rAddrUdp := &net.UDPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	for i := 0; i < nm.conf.NrTestMessagesToSend; i++ {
		//nm.logger.Infof("Writing test message to: %s", p.ToString())
		_, err := nm.udpConn.WriteToUDP(analytics.SerializeHeartbeatMessage(analytics.NewHBMessageForceReply(nm.selfPeer, true)), rAddrUdp)
		if err != nil {
			panic(err)
		}
	}

	select {
	case <-nodeInfo.enoughTestMessages:
		return &nm.udpConn, nil
	case <-nodeInfo.unwatch:
		return &nm.udpConn, errors.NonFatalError(500, "error occurred establishing udp conn", nodeWatcherCaller)
	case <-time.After(time.Until(udpTestDeadline)):
		nm.logger.Warnf("falling back to TCP")
	}

	rAddrTcp := &net.TCPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	tcpConn, err := net.DialTimeout("tcp", rAddrTcp.String(), nm.conf.TcpTestTimeout)
	if err != nil {
		return nil, errors.NonFatalError(500, "failed to establish connection", nodeWatcherCaller)
	}
	return tcpConn, nil
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
		go nm.handleTCPConnection(newStream)
	}
}

func (nm *NodeWatcherImpl) handleTCPConnection(c *net.TCPConn) {
	defer c.Close()

	frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, c)

	for {
		frame, rErr := frameBasedConn.ReadFrame()
		if rErr != nil {
			nm.logger.Warn(rErr)
			return
		}
		err := nm.handleHBMessageTCP(frame, frameBasedConn)
		if err != nil {
			nm.logger.Warn(err.Reason())
			return
		}
	}
}
func (nm *NodeWatcherImpl) handleHBMessageTCP(hbBytes []byte, mw messageIO.FrameConn) errors.Error {
	hb := analytics.DeserializeHeartbeatMessage(hbBytes)
	// nm.logger.Infof("handling hb message via TCP :%+v", hb)
	nodeInfoInt, ok := nm.watching.Load(hb.Sender.String())
	if !ok {
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	nodeInfo := nodeInfoInt.(nodeWatchingValue)

	if hb.IsReply {
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

		err := mw.WriteFrame(analytics.SerializeHeartbeatMessage(toSend))
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
		nodeInfoInt, ok := nm.watching.Load(hb.Sender.String())
		if !ok {
			return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
		}
		nodeInfo := nodeInfoInt.(nodeWatchingValue)
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

func (nm *NodeWatcherImpl) registerHBReply(hb analytics.HeartbeatMessage, nodeInfo *NodeInfoImpl) {

	defer func() {
		if r := recover(); r != nil {
			nm.logger.Error("Recovered in f", r)
		}
	}()

	if !hb.Initial {
		nodeInfo.Detector().Heartbeat()
	}
	currMeasurement := int(atomic.AddInt32(nodeInfo.nrMessagesReceived, 1))

	timeTaken := time.Since(hb.TimeStamp)
	nodeInfo.LatencyCalc().AddMeasurement(timeTaken)

	if currMeasurement == nm.conf.MinSamplesLatencyEstimate {
		select {
		case <-nodeInfo.enoughSamples:
		default:
			close(nodeInfo.enoughSamples)
		}
	}

	if currMeasurement == nm.conf.NrTestMessagesToReceive {
		select {
		case <-nodeInfo.enoughTestMessages:
		default:
			close(nodeInfo.enoughTestMessages)
		}
	}

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

func (nm *NodeWatcherImpl) NotifyOnCondition(c nodeWatcher.Condition) (int, errors.Error) {
	condId := rand.Int()
	if reflect.ValueOf(c.Peer).IsNil() {
		nm.logger.Panic("peer to notify is nil")
	}
	conditionsItem := &priorityqueue.Item{
		Value:    c,
		Key:      condId,
		Priority: time.Now().Add(c.EvalConditionTickDuration).Unix(),
	}
	nm.logger.Infof("Adding condition %+v for peer %s", c, c.Peer.String())
	nm.addCondChan <- conditionsItem
	return condId, nil
}

func (nm *NodeWatcherImpl) evalCondsPeriodic() {

LOOP:
	for {
		var waitTime time.Duration
		var nextItem *priorityqueue.Item
		heap.Init(&nm.conditions)
		var nextItemTimer = time.NewTimer(math.MaxInt64)
		if nm.conditions.Len() > 0 {
			nextItem = heap.Pop(&nm.conditions).(*priorityqueue.Item)
			waitTime = time.Until(time.Unix(0, nextItem.Priority))
			nextItemTimer = time.NewTimer(waitTime)
		}
		select {
		case newItem := <-nm.addCondChan:
			// nm.logger.Infof("Received add condition signal...")
			nm.logger.Infof("Adding condition %d", newItem.Key)
			heap.Push(&nm.conditions, newItem)
			if nextItem != nil {
				// nm.logger.Infof("nextItem (%d) was not nil, re-adding to condition list", nextItem.Key)
				heap.Push(&nm.conditions, nextItem)
			}

		case toRemove := <-nm.removeNodeCondsChan:
			nm.logger.Infof("removing conds from node: %s", toRemove.String())
			// nm.logger.Infof("conds: %+v", nm.conditions)
			if nextItem != nil {
				heap.Push(&nm.conditions, nextItem)
			}

			for idx, cond := range nm.conditions {
				// nm.logger.Infof("cond: %+v", cond)
				if cond != nil {
					if PeersEqual(cond.Value.(nodeWatcher.Condition).Peer, toRemove) {
						heap.Remove(&nm.conditions, idx)
					}
				}
			}
			heap.Init(&nm.conditions)
		case req := <-nm.cancelCondChan:
			// nm.logger.Infof("Received cancel cond signal...")
			if req.key == nextItem.Key {
				nm.logger.Infof("Removed condition %d successfully", req.key)

				req.removed <- req.key
				// nm.logger.Infof("Removed condition %d successfully", req.key)
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
			// nm.conditions.LogEntries(nm.logger)
		case <-nextItemTimer.C:
			cond := nextItem.Value.(nodeWatcher.Condition)
			nodeInfoInt, ok := nm.watching.Load(cond.Peer.String())
			if ok { // remove all conditions from unwatched nodes
				nodeInfo := nodeInfoInt.(nodeWatchingValue)
				select {
				case <-nodeInfo.enoughSamples:
					if cond.CondFunc(nodeInfo) {
						nm.logger.Infof("Condition trigger: %+v", *nextItem)
						go nm.babel.SendNotification(cond.Notification)
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
					} else {
						nextItem.Priority = time.Now().Add(cond.EvalConditionTickDuration).UnixNano()
						heap.Push(&nm.conditions, nextItem)
					}
				default:
					nextItem.Priority = time.Now().Add(cond.EvalConditionTickDuration).UnixNano()
					heap.Push(&nm.conditions, nextItem)
				}
			} else {
				nm.logger.Infof("Condition Node not watched %+v", *nextItem)
			}
		}
		// nm.conditions.LogEntries(nm.logger)
	}
}

func (nm *NodeWatcherImpl) removeCond(condId int) int {
	// nm.logger.Infof("Canceling timer with ID %d", condId)
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
		nm.watching.Range(func(k, v interface{}) bool {
			watchedPeer := v.(nodeWatchingValue)
			select {
			case <-watchedPeer.enoughSamples:
				nm.logger.Infof("Node %s:, PHI: %f, Latency: %d, Subscribers: %d", watchedPeer.Peer().String(), watchedPeer.Detector().Phi(), watchedPeer.LatencyCalc().CurrValue(), len(watchedPeer.subscribers))
			default:
			}
			return true
		})

	}
}
