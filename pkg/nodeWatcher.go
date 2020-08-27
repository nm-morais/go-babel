package pkg

import (
	"io"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/analytics"
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
}

type NodeInfo struct {
	mw            io.Writer
	mr            io.Reader
	nrRetries     int
	subscribers   map[protocol.ID]bool
	peer          peer.Peer
	peerConn      net.Conn
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

	conditions     analytics.PriorityQueue
	conditionsLock *sync.RWMutex
	reEvalNextCond chan *struct{}

	logger *logrus.Logger
}

type NodeWatcherConf struct {
	EvalConditionTickDuration time.Duration
	MaxRedials                int
	TcpTestTimeout            time.Duration
	UdpTestTimeout            time.Duration
	NrTestMessages            int
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
	GetNodeInfoWithTimeout(peer peer.Peer, timeout time.Duration) (*NodeInfo, errors.Error)
	NotifyOnCondition(p peer.Peer, notification notification.Notification, cond ConditionFunc, evalConditionTickDuration time.Duration, protoID protocol.ID) (int, errors.Error)
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

		conditions:     make(analytics.PriorityQueue, 0),
		conditionsLock: &sync.RWMutex{},
		reEvalNextCond: make(chan *struct{}),

		logger: logs.NewLogger(nodeWatcherCaller),
	}
	nm.logger.Infof("My configs: %+v", config)
	go nm.start()
	return nm
}

func (nm *NodeWatcherImpl) Watch(p peer.Peer, protoID protocol.ID) errors.Error {
	nm.watchingLock.Lock()
	if _, ok := nm.watching[p.ToString()]; ok {
		nm.watchingLock.Unlock()
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}
	nm.watchingLock.Unlock()

	nm.dialingLock.RLock()
	if nm.dialing[p.ToString()] {
		nm.dialingLock.RUnlock()
		return nil
	}
	nm.dialingLock.RUnlock()

	nm.dialingLock.Lock()
	if nm.dialing[p.ToString()] {
		nm.dialingLock.Unlock()
		return nil
	}
	nm.dialing[p.ToString()] = true
	nm.dialingLock.Unlock()

	//TODO make conn
	stream, err := nm.establishStreamTo(p)
	if err != nil {
		nm.dialingLock.Lock()
		defer nm.dialingLock.Unlock()
		delete(nm.dialing, p.ToString())
		return errors.NonFatalError(500, err.Reason(), nodeWatcherCaller)
	}

	nm.watchingLock.Lock()
	nm.watching[p.ToString()] = NodeInfo{
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

	switch stream.(type) {
	case *net.TCPConn:
		go nm.handleTCPConnection(stream.(*net.TCPConn))
	case *net.UDPConn:
		go nm.handleUDPConnection(stream.(*net.UDPConn))
	}
	go nm.startHbRoutine(p)
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
		lat, err := nodeInfo.LatencyCalc.CurrValue()
		if err == nil {
			nm.logger.Infof("Node %s: ConnType: %s PHI: %f, Latency: %d, Subscribers: %d ", nodeInfo.peer.ToString(), reflect.TypeOf(nodeInfo.peerConn), nodeInfo.Detector.Phi(time.Now()), lat, len(nodeInfo.subscribers))
		}

		toSend := analytics.NewHBMessageForceReply(nm.selfPeer)
		switch nodeInfo.peerConn.(type) {
		case net.PacketConn:
			//nm.logger.Infof("sending hb message:%+v", toSend)
			nm.logger.Infof("Sent HB to %s via UDP", nodeInfo.peer.ToString())
			_, err := nodeInfo.peerConn.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		case net.Conn:
			nm.logger.Infof("Sent HB to %s via TCP", nodeInfo.peer.ToString())
			_, err := nodeInfo.mw.Write(analytics.SerializeHeartbeatMessage(toSend))
			if err != nil {
				nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		}
	}
}

func (nm *NodeWatcherImpl) Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error {
	nm.watchingLock.Lock()
	defer nm.watchingLock.Unlock()
	watchedPeer, ok := nm.watching[peer.ToString()]
	if ok {
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	delete(nm.watching, peer.ToString())
	close(watchedPeer.enoughSamples)
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

func (nm *NodeWatcherImpl) GetNodeInfoWithTimeout(peer peer.Peer, timeout time.Duration) (*NodeInfo, errors.Error) {
	nm.watchingLock.Lock()
	nodeInfo, ok := nm.watching[peer.ToString()]
	if !ok {
		nm.watchingLock.Unlock()
		return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	nm.watchingLock.Unlock()

	select {
	case <-nodeInfo.enoughSamples:
		nm.watchingLock.Lock() // re-check if node died in the meantime
		nodeInfo, ok := nm.watching[peer.ToString()]
		if !ok {
			nm.watchingLock.Unlock()
			return nil, errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
		}
		nm.watchingLock.Unlock()
		return &nodeInfo, nil
	case <-time.After(timeout):
		return nil, errors.NonFatalError(404, "timed out waiting for enough samples", nodeWatcherCaller)
	}
}

func (nm *NodeWatcherImpl) NotifyOnCondition(p peer.Peer, notification notification.Notification, cond ConditionFunc, evalConditionTickDuration time.Duration, protoID protocol.ID) (int, errors.Error) {
	nm.conditionsLock.Lock()
	condId := rand.Int()
	newCond := Condition{
		EvalConditionTickDuration: evalConditionTickDuration,
		CondFunc:                  cond,
		ProtoId:                   protoID,
		Notification:              notification,
	}
	pqItem := &analytics.Item{
		Value:    newCond,
		Priority: time.Until(time.Now().Add(evalConditionTickDuration)).Nanoseconds(),
		Key:      condId,
	}
	nm.conditions.Push(pqItem)
	if pqItem.Index == 0 {
		nm.reEvalNextCond <- nil
	}
	nm.conditionsLock.Unlock()
	return condId, nil
}

func (nm *NodeWatcherImpl) evalCondsPeriodic(c Condition) errors.Error {
	for {
		nm.conditionsLock.Lock()
		nextItem := nm.conditions.Pop().(*analytics.Item)
		cond := nextItem.Value.(Condition)
		nm.conditionsLock.Unlock()

		select {
		case <-nm.reEvalNextCond:
			nm.conditionsLock.Lock()
			nm.conditions.Push(nextItem)
			nm.conditionsLock.Unlock()
			continue
		case <-time.After(time.Duration(nextItem.Priority)):
			nm.watchingLock.Lock()
			nodeStats := nm.watching[cond.Peer.ToString()]
			nm.watchingLock.Unlock()
			if cond.CondFunc(nodeStats) {
				SendNotification(cond.Notification)
			}
			nm.conditions.Update(nextItem, cond, time.Until(time.Now().Add(cond.EvalConditionTickDuration)).Nanoseconds())
		}
	}
}

func (nm *NodeWatcherImpl) Logger() *logrus.Logger {
	return nm.logger
}

func (nm *NodeWatcherImpl) establishStreamTo(p peer.Peer) (net.Conn, errors.Error) {

	//nm.logger.Infof("Establishing stream to %s", p.ToString())
	testMsg := analytics.HeartbeatMessage{Sender: nm.selfPeer, IsTest: true, IsReply: false, ForceReply: false}
	udpTestDeadline := time.Now().Add(nm.conf.UdpTestTimeout)
	rAddrUdp := &net.UDPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	udpConn, err := net.DialUDP("udp", nil, rAddrUdp)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}

	for i := 0; i < nm.conf.NrTestMessages; i++ {
		//nm.logger.Infof("Writing test message to: %s", p.ToString())
		_, err := udpConn.Write(analytics.SerializeHeartbeatMessage(testMsg))
		if err != nil {
			break
		}

		udpConn.SetReadDeadline(udpTestDeadline)
		receiveBuf := make([]byte, 100)
		//nm.logger.Infof("Reading test message from: %s", p.ToString())
		_, _, err = udpConn.ReadFromUDP(receiveBuf)
		if err != nil {
			nm.logger.Warnf("Read error: %s", err.Error())
			break
		}
		udpConn.SetReadDeadline(time.Time{})
		// managed to read a message using tcp
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
		go nm.handleHBMessageTCP(msgBuf[:n], mw)
	}
}

func (nm *NodeWatcherImpl) handleUDPConnection(inConn *net.UDPConn) {
	rAddr := inConn.RemoteAddr().(*net.UDPAddr)
	for {
		msgBuf := make([]byte, 2048)
		n, rErr := inConn.Read(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			return
		}
		go nm.handleHBMessageUDP(msgBuf[:n], inConn, rAddr)
	}
}

func (nm *NodeWatcherImpl) handleHBMessageTCP(hbBytes []byte, mw io.Writer) errors.Error {
	hb := analytics.DeserializeHeartbeatMessage(hbBytes)
	if hb.IsTest {
		//nm.logger.Infof("handling hb test message %+v", hb)
		_, err := mw.Write(analytics.SerializeHeartbeatMessage(hb))
		if err != nil {
			nm.logger.Error(err)
		}
		return nil
	}
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
			IsTest:     false,
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
	if hb.IsTest {
		//nm.logger.Infof("handling hb test message, sending it to %s", rAddr.String())
		_, err := udpConn.WriteTo(analytics.SerializeHeartbeatMessage(hb), rAddr)
		if err != nil {
			nm.logger.Error(err)
		}
		return nil
	}
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
			IsTest:     false,
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
	nm.watchingLock.Lock()
	nodeInfo, ok := nm.watching[sender.ToString()]
	if !ok {
		nm.watchingLock.Unlock()
		nm.logger.Warn("Received reply for unwatched node, discarding...")
		return
	}
	nodeInfo.Detector.Ping(timeReceived)
	nodeInfo.LatencyCalc.AddMeasurement(timeTaken)
	if nodeInfo.Detector.NrSamples() >= nm.conf.MinSamplesFaultDetector && nodeInfo.LatencyCalc.NrMeasurements() >= nm.conf.MinSamplesLatencyEstimate {
		close(nodeInfo.enoughSamples)
	}
	nm.watchingLock.Unlock()
}
