package pkg

import (
	"container/heap"
	"io"
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
	mw            io.Writer
	mr            io.Reader
	nrRetries     int
	subscribers   map[protocol.ID]bool
	peer          peer.Peer
	peerConn      net.Conn
	mu            *sync.Mutex
	enoughSamples chan interface{}
	LatencyCalc   *analytics.LatencyCalculator
	Detector      *analytics.Detector
}

type NodeWatcherImpl struct {
	selfPeer peer.Peer
	conf     NodeWatcherConf

	dialing     map[string]bool
	dialingLock *sync.RWMutex

	watching     map[string]NodeInfo
	watchingLock *sync.RWMutex

	conditions     dataStructures.PriorityQueue
	conditionsLock *sync.RWMutex
	reEvalNextCond chan *struct{}

	logger *logrus.Logger
}

type NodeWatcherConf struct {
	EvalConditionTickDuration time.Duration
	MaxRedials                int
	TcpTestTimeout            time.Duration
	UdpTestTimeout            time.Duration
	NrTestMessagesToSend      int
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
	Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error
	GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error)
	GetNodeInfoWithDeadline(peer peer.Peer, deadline time.Time) (*NodeInfo, errors.Error)
	NotifyOnCondition(c Condition) (int, errors.Error)
	CancelCond(condID int) errors.Error
	Logger() *logrus.Logger
}

func NewNodeWatcher(selfPeer peer.Peer, config NodeWatcherConf) NodeWatcher {
	nm := &NodeWatcherImpl{
		conf:     config,
		selfPeer: selfPeer,

		watching:     make(map[string]NodeInfo),
		watchingLock: &sync.RWMutex{},

		dialing:     make(map[string]bool),
		dialingLock: &sync.RWMutex{},

		conditions:     make(dataStructures.PriorityQueue, 0),
		conditionsLock: &sync.RWMutex{},
		reEvalNextCond: make(chan *struct{}),

		logger: logs.NewLogger(nodeWatcherCaller),
	}
	nm.logger.Infof("My configs: %+v", config)
	nm.start()
	return nm
}

func (nm *NodeWatcherImpl) Watch(p peer.Peer, protoID protocol.ID) errors.Error {
	nm.logger.Infof("Proto %d request to watch %s", protoID, p.ToString())
	nm.watchingLock.Lock()
	if _, ok := nm.watching[p.ToString()]; ok {
		nm.watchingLock.Unlock()
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	nm.watching[p.ToString()] = NodeInfo{
		subscribers:   map[protocol.ID]bool{protoID: true},
		peer:          p,
		enoughSamples: make(chan interface{}),
	}
	nm.watchingLock.Unlock()
	go func() {

		nm.dialingLock.RLock()
		if nm.dialing[p.ToString()] {
			nm.dialingLock.RUnlock()
			return
		}
		nm.dialingLock.RUnlock()

		nm.dialingLock.Lock()
		if nm.dialing[p.ToString()] {
			nm.dialingLock.Unlock()
			return
		}
		nm.dialing[p.ToString()] = true
		nm.dialingLock.Unlock()

		//TODO make conn
		stream, err := nm.establishStreamTo(p)
		if err != nil {
			nm.dialingLock.Lock()
			defer nm.dialingLock.Unlock()
			delete(nm.dialing, p.ToString())
			return
		}

		nm.watchingLock.Lock()
		nm.watching[p.ToString()] = NodeInfo{
			mu:            &sync.Mutex{},
			subscribers:   map[protocol.ID]bool{protoID: true},
			peerConn:      stream,
			peer:          p,
			LatencyCalc:   analytics.NewLatencyCalculator(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight),
			Detector:      analytics.NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
			mr:            messageIO.NewMessageReader(stream),
			mw:            messageIO.NewMessageWriter(stream),
			enoughSamples: make(chan interface{}),
		}
		nm.watchingLock.Unlock()
		nm.logWatchingNodes()
		switch stream := stream.(type) {
		case *net.TCPConn:
			go nm.handleTCPConnection(stream)
		case *net.UDPConn:
			go nm.handleUDPConnection(stream)
		}
		nm.startHbRoutine(p)
	}()
	return nil
}

func (nm *NodeWatcherImpl) startHbRoutine(p peer.Peer) {
	ticker := time.NewTicker(nm.conf.HbTickDuration)
	for {
		<-ticker.C
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[p.ToString()]
		if !ok {
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
	close(watchedPeer.enoughSamples)
	nm.watchingLock.Unlock()
	return nil
}

func (nm *NodeWatcherImpl) GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error) {
	nm.watchingLock.Lock()
	defer nm.watchingLock.Unlock()
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

func (nm *NodeWatcherImpl) NotifyOnCondition(c Condition) (int, errors.Error) {
	nm.conditionsLock.Lock()
	condId := rand.Int()
	pqItem := &dataStructures.Item{
		Value:    c,
		Priority: time.Now().Add(c.EvalConditionTickDuration).UnixNano(),
		Key:      condId,
	}
	heap.Push(&nm.conditions, pqItem)
	if pqItem.Index == 0 {
		nm.reEvalNextCond <- nil
	}
	nm.conditionsLock.Unlock()
	return condId, nil
}

func (nm *NodeWatcherImpl) evalCondsPeriodic() errors.Error {
	for {
		var nextItem *dataStructures.Item
		nm.conditionsLock.Lock()
		if nm.conditions.Len() > 0 {
			nextItem = heap.Pop(&nm.conditions).(*dataStructures.Item)
		}
		nm.conditionsLock.Unlock()

		if nextItem == nil {
			<-nm.reEvalNextCond
			continue
		}
		cond := nextItem.Value.(Condition)
		select {
		case <-nm.reEvalNextCond:
			nm.conditionsLock.Lock()
			heap.Push(&nm.conditions, nextItem)
			nm.conditionsLock.Unlock()
			continue
		case <-time.After(time.Until(time.Unix(0, nextItem.Priority))):
			nm.watchingLock.Lock()
			nodeStats := nm.watching[cond.Peer.ToString()]
			nm.watchingLock.Unlock()
			if cond.CondFunc(nodeStats) {
				SendNotification(cond.Notification)
				if cond.Repeatable {
					if cond.EnableGracePeriod {
						nm.conditions.Update(nextItem, cond, time.Now().Add(cond.GracePeriod).UnixNano())
						continue
					}
					nm.conditions.Update(nextItem, cond, time.Now().Add(cond.EvalConditionTickDuration).UnixNano())
				}
			}
		}
	}
}

func (nm *NodeWatcherImpl) CancelCond(condID int) errors.Error {
	panic("not yet implemented")
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
	if hb.IsReply {
		//nm.logger.Infof("handling hb reply message %+v", hb)
		nm.registerHBReply(hb)
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
		//nm.logger.Infof("handling hb reply message")
		nm.registerHBReply(hb)
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

func (nm *NodeWatcherImpl) registerHBReply(hb analytics.HeartbeatMessage) {
	sender := hb.Sender
	timeReceived := time.Now()
	timeTaken := time.Since(hb.TimeStamp)
	nm.watchingLock.RLock()
	nodeInfo, ok := nm.watching[sender.ToString()]
	if !ok {
		nm.watchingLock.RUnlock()
		nm.logger.Warn("Received reply for unwatched node, discarding...")
		return
	}
	nm.watchingLock.RUnlock()

	nodeInfo.mu.Lock()
	defer nodeInfo.mu.Unlock()

	nodeInfo.Detector.Ping(timeReceived)
	nodeInfo.LatencyCalc.AddMeasurement(timeTaken)
	if nodeInfo.Detector.NrSamples() == nm.conf.MinSamplesFaultDetector && nodeInfo.LatencyCalc.NrMeasurements() == nm.conf.MinSamplesLatencyEstimate {
		select {
		case <-nodeInfo.enoughSamples:
		default:
			nm.logger.Infof("Closed enoughSamples chan for peer %s", sender.ToString())
			lat := nodeInfo.LatencyCalc.CurrValue()
			nm.logger.Infof("Node %s: ConnType: %s PHI: %f, Latency: %d, Subscribers: %d ", nodeInfo.peer.ToString(), reflect.TypeOf(nodeInfo.peerConn), nodeInfo.Detector.Phi(time.Now()), lat, len(nodeInfo.subscribers))
			close(nodeInfo.enoughSamples)
		}
	}
}

func (nm *NodeWatcherImpl) logWatchingNodes() {
	nm.watchingLock.RLock()
	for _, p := range nm.watching {
		nm.logger.Infof("Watching peer %s", p.peer.ToString())
	}
	nm.watchingLock.RUnlock()
}
