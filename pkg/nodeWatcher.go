package pkg

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/analytics"
	timerQueue "github.com/nm-morais/go-babel/pkg/dataStructures/timedEventQueue"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/nodeWatcher"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/sirupsen/logrus"
)

const nodeWatcherCaller = "NodeWatcher"

type NodeInfoImpl struct {
	peerConn           net.Conn
	nrMessagesReceived *int32

	enoughTestMessages chan interface{}
	enoughSamples      chan interface{}
	err                chan interface{}
	unwatch            chan interface{}

	peer        peer.Peer
	latencyCalc *analytics.LatencyCalculator
	detector    *analytics.PhiAccuralFailureDetector

	nw *NodeWatcherImpl
}

func (n *NodeInfoImpl) OnTrigger() (bool, *time.Time) {
	select {
	case <-n.unwatch:
		return false, nil
	default:
	}

	toSend := analytics.NewHBMessageForceReply(n.nw.selfPeer, false)
	switch conn := n.peerConn.(type) {
	case net.PacketConn:
		rAddrUdp := &net.UDPAddr{IP: n.Peer().IP(), Port: int(n.Peer().AnalyticsPort())}
		// n.nw.logger.Infof("sending hb message:%+v", toSend)
		// n.nw.logger.Infof("Sent HB to %s via UDP", n.peer.String())
		_, err := n.nw.udpConn.WriteToUDP(analytics.SerializeHeartbeatMessage(toSend), rAddrUdp)
		if err != nil {
			n.nw.logger.Panicf("err %s in udp conn", err.Error())
		}
	case net.Conn:
		frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, conn)

		toSend := analytics.NewHBMessageForceReply(n.nw.selfPeer, false)
		err := frameBasedConn.WriteFrame(analytics.SerializeHeartbeatMessage(toSend))
		if err != nil {
			close(n.err)
			return false, nil
		}
	}
	nextTrigger := time.Now().Add(n.nw.conf.HbTickDuration)
	return true, &nextTrigger
}

func (n *NodeInfoImpl) ID() string {
	return n.peer.String()
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

type ConditionWrapper struct {
	id          string
	lastTrigger time.Time
	c           nodeWatcher.Condition
	nm          *NodeWatcherImpl
}

func (cw *ConditionWrapper) ID() string {
	return cw.id
}

func (cw *ConditionWrapper) OnTrigger() (bool, *time.Time) {
	cond := cw.c
	nodeInfoInt, ok := cw.nm.watching.Load(cond.Peer.String())
	if !ok { // remove all conditions from unwatched nodes
		return false, nil
	}
	nodeInfo := nodeInfoInt.(nodeWatchingValue)
	select {
	case <-nodeInfo.enoughSamples:
		triggerCond := cond.CondFunc(nodeInfo)
		if triggerCond {
			cw.nm.logger.Infof("Condition trigger: %+v", cw.c)
			cw.nm.babel.SendNotification(cond.Notification)
			if !cond.Repeatable {
				return false, nil
			}
		}
		nextTrigger := time.Now().Add(cond.EvalConditionTickDuration)
		if cw.c.EnableGracePeriod {
			nextTrigger = time.Now().Add(cond.GracePeriod)
		}
		return true, &nextTrigger
	default:
		nextTrigger := time.Now().Add(cond.EvalConditionTickDuration)
		return true, &nextTrigger
	}
}

type nodeWatchingValue = *NodeInfoImpl

type NodeWatcherImpl struct {
	babel protocolManager.ProtocolManager

	selfPeer peer.Peer
	conf     NodeWatcherConf

	sentCounterMux     *sync.Mutex
	receivedCounterMux *sync.Mutex
	msgCountersSent    int64
	msgCountersRecvd   int64

	watching      sync.Map
	udpConn       net.UDPConn
	conditionsTeq timerQueue.TimedEventQueue
	hbTeq         timerQueue.TimedEventQueue
	logger        *logrus.Logger
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

	AdvertiseListenAddr net.IP

	ListenAddr net.IP
	ListenPort int
}

func NewNodeWatcher(config NodeWatcherConf, babel protocolManager.ProtocolManager) nodeWatcher.NodeWatcher {
	logger := logs.NewLogger(nodeWatcherCaller)
	nm := &NodeWatcherImpl{
		babel:              babel,
		selfPeer:           peer.NewPeer(config.AdvertiseListenAddr, babel.SelfPeer().ProtosPort(), uint16(config.ListenPort)),
		conf:               config,
		sentCounterMux:     &sync.Mutex{},
		receivedCounterMux: &sync.Mutex{},
		msgCountersSent:    0,
		msgCountersRecvd:   0,
		watching:           sync.Map{},
		udpConn:            net.UDPConn{},
		conditionsTeq:      timerQueue.NewTimedEventQueue(logger),
		hbTeq:              timerQueue.NewTimedEventQueue(logger),
		logger:             logger,
	}

	nm.logger.Infof("Starting nodeWatcher with config: %+v", config)

	if nm.conf.OldLatencyWeight+nm.conf.NewLatencyWeight != 1 {
		nm.logger.Panic("OldLatencyWeight + NewLatencyWeight != 1")
	}

	if config.AdvertiseListenAddr == nil {
		nm.selfPeer = peer.NewPeer(config.ListenAddr, babel.SelfPeer().ProtosPort(), uint16(config.ListenPort))
	}

	listenAddr := &net.UDPAddr{
		IP:   nm.conf.ListenAddr,
		Port: nm.conf.ListenPort,
	}

	udpConn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		nm.logger.Panic(err)
	}
	nm.udpConn = *udpConn
	nm.logger.Infof("My configs: %+v", config)
	nm.logger.Infof("Listening on addr %+v and port %d", nm.conf.ListenAddr, nm.conf.ListenPort)
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
		nm.logger.Panic("Tried to watch nil peer")
	}

	nm.logger.Infof("Proto %d request to watch %s on port %d", issuerProto, p.String(), p.AnalyticsPort())

	var nrMessages int32 = 0
	detector, err := analytics.New(
		nm.conf.PhiThreshold,
		uint(nm.conf.WindowSize),
		nm.conf.MinStdDeviation,
		nm.conf.AcceptableHbPause,
		nm.conf.FirstHeartbeatEstimate,
		nil,
	)

	if err != nil {
		return errors.FatalError(409, err.Error(), nodeWatcherCaller)
	}

	nodeInfo := &NodeInfoImpl{
		peerConn:           nil,
		nrMessagesReceived: &nrMessages,
		enoughTestMessages: make(chan interface{}),
		enoughSamples:      make(chan interface{}),
		err:                make(chan interface{}),
		unwatch:            make(chan interface{}),
		peer:               p,
		latencyCalc:        analytics.NewLatencyCalculator(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight),
		detector:           detector,
		nw:                 nm,
	}

	_, loaded := nm.watching.LoadOrStore(p.String(), nodeInfo)
	if loaded {
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}

	go nm.dialAndWatch(issuerProto, nodeInfo)
	return nil
}

func (nm *NodeWatcherImpl) WatchWithInitialLatencyValue(p peer.Peer, issuerProto protocol.ID, latency time.Duration) errors.Error {
	nm.logger.Infof("Proto %d request to watch %s with initial value %s on port %d", issuerProto, p.String(), latency, p.AnalyticsPort())

	if reflect.ValueOf(p).IsNil() {
		nm.logger.Panic("Tried to watch nil peer")
	}

	if _, ok := nm.watching.Load(p.String()); ok {
		return errors.NonFatalError(409, "peer already being tracked", nodeWatcherCaller)
	}

	var nrMessages int32 = 0
	detector, err := analytics.New(
		nm.conf.PhiThreshold,
		uint(nm.conf.WindowSize),
		nm.conf.MinStdDeviation,
		nm.conf.AcceptableHbPause,
		nm.conf.FirstHeartbeatEstimate,
		nil,
	)

	if err != nil {
		nm.logger.Panic(err)
	}

	newNodeInfo := &NodeInfoImpl{
		peerConn:           nil,
		enoughTestMessages: make(chan interface{}),
		enoughSamples:      make(chan interface{}),
		nrMessagesReceived: &nrMessages,
		err:                make(chan interface{}),
		unwatch:            make(chan interface{}),
		peer:               p,
		latencyCalc:        analytics.NewLatencyCalculatorWithValue(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight, latency),
		detector:           detector,
		nw:                 nm,
	}
	close(newNodeInfo.enoughSamples)
	close(newNodeInfo.enoughTestMessages)
	newNodeInfo.latencyCalc.AddMeasurement(latency)
	nm.watching.Store(p.String(), newNodeInfo)
	go nm.dialAndWatch(issuerProto, newNodeInfo)
	return nil
}

func (nm *NodeWatcherImpl) startHbRoutine(nodeInfo *NodeInfoImpl) {
	timeNow := time.Now()
	nm.hbTeq.Add(nodeInfo, timeNow)
}

func (nm *NodeWatcherImpl) Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error {
	nm.logger.Infof("Proto %d request to unwatch %s", protoID, peer.String())
	watchedPeer, loaded := nm.watching.LoadAndDelete(peer.String())
	if !loaded {
		nm.logger.Warn("peer not being tracked")
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	close(watchedPeer.(nodeWatchingValue).unwatch)
	nm.watching.Delete(peer.String())
	_ = nm.hbTeq.Remove(peer.String())
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

func (nm *NodeWatcherImpl) GetNodeInfoWithDeadline(peer peer.Peer, deadline time.Time) (
	nodeWatcher.NodeInfo,
	errors.Error,
) {
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
	nrErrors := 0
	for i := 0; i < nm.conf.NrTestMessagesToSend; i++ {

		//nm.logger.Infof("Writing test message to: %s", p.ToString())
		_, err := nm.udpConn.WriteToUDP(
			analytics.SerializeHeartbeatMessage(
				analytics.NewHBMessageForceReply(
					nm.selfPeer,
					true,
				),
			), rAddrUdp,
		)
		if err != nil {
			if nrErrors == 3 {
				nm.logger.Panic(err)
			}
			nrErrors++
			nm.logger.Warnf("Error sending udp message: %s", err.Error())
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
	go nm.printLatencyToPeriodic()
}

func (nm *NodeWatcherImpl) startUDPServer() {
	msgBuf := make([]byte, 200)
	for {
		n, _, rErr := nm.udpConn.ReadFromUDP(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			continue
		}
		nm.handleHBMessageUDP(msgBuf[:n])
	}
}

func (nm *NodeWatcherImpl) startTCPServer() {
	listenAddr := &net.TCPAddr{
		IP:   nm.conf.ListenAddr,
		Port: nm.conf.ListenPort,
	}

	listener, err := net.ListenTCP("tcp", listenAddr)
	if err != nil {
		nm.logger.Panic(err)
		panic(err)
	}

	for {
		newStream, err := listener.AcceptTCP()
		if err != nil {
			nm.logger.Panic(err)
		}
		go nm.handleTCPConnection(newStream)
	}
}

func (nm *NodeWatcherImpl) handleTCPConnection(c *net.TCPConn) {
	defer func() {
		err := c.Close()
		if err != nil {
			nm.logger.Errorf("error closing connection: %w", err)
		}
	}()

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
	nm.addMsgReceived()

	if hb.IsReply {
		nodeInfoInt, ok := nm.watching.Load(hb.Sender.String())
		if !ok {
			return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
		}
		nodeInfo := nodeInfoInt.(nodeWatchingValue)
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
	nm.logger.Panic("Should not be here")
	return nil
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
	nm.logger.Panic("Should not be here")
	return nil
}

func (nm *NodeWatcherImpl) addMsgSent() {
	nm.sentCounterMux.Lock()
	nm.msgCountersSent++
	nm.sentCounterMux.Unlock()
}

func (nm *NodeWatcherImpl) addMsgReceived() {
	nm.receivedCounterMux.Lock()
	nm.msgCountersRecvd++
	nm.receivedCounterMux.Unlock()
}

func (nm *NodeWatcherImpl) registerHBReply(hb analytics.HeartbeatMessage, nodeInfo *NodeInfoImpl) {

	if !hb.Initial {
		nodeInfo.Detector().Heartbeat()
	}
	currMeasurement := int(atomic.AddInt32(nodeInfo.nrMessagesReceived, 1))
	timeTaken := time.Since(hb.TimeStamp)
	nodeInfo.LatencyCalc().AddMeasurement(timeTaken)

	if nodeInfo.enoughSamples != nil && currMeasurement == nm.conf.MinSamplesLatencyEstimate {
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

func (nm *NodeWatcherImpl) CancelCond(condID string) errors.Error {
	responseChan := make(chan string)
	defer close(responseChan)
	ok := nm.conditionsTeq.Remove(condID)
	if !ok {
		return errors.NonFatalError(404, "condition not found", nodeWatcherCaller)
	}
	return nil
}

func (nm *NodeWatcherImpl) NotifyOnCondition(c nodeWatcher.Condition) (string, errors.Error) {
	condId := fmt.Sprintf("%d", rand.Int())
	if reflect.ValueOf(c.Peer).IsNil() {
		nm.logger.Panic("peer to notify is nil")
	}
	conditionWrapper := &ConditionWrapper{
		id:          condId,
		lastTrigger: time.Time{},
		c:           c,
		nm:          nm,
	}
	nm.conditionsTeq.Add(conditionWrapper, time.Now().Add(c.EvalConditionTickDuration))
	nm.logger.Infof("Adding condition %+v for peer %s", c, c.Peer.String())
	return condId, nil
}

func (nm *NodeWatcherImpl) printControlMessagesSent() {
	for range time.NewTicker(10 * time.Second).C {
		nm.receivedCounterMux.Lock()
		defer nm.receivedCounterMux.Unlock()
		nm.sentCounterMux.Lock()
		defer nm.sentCounterMux.Unlock()
		toPrint := struct {
			ControlMessagesSent     int
			ControlMessagesReceived int
		}{
			ControlMessagesSent:     int(nm.msgCountersSent),
			ControlMessagesReceived: int(nm.msgCountersRecvd),
		}

		toPrint_json, err := json.Marshal(toPrint)
		if err != nil {
			panic(err)
		}
		nm.logger.Infof("<control-messages-stats>:%s", string(toPrint_json))
	}
}

func (nm *NodeWatcherImpl) printLatencyToPeriodic() {
	if nm.conf.PrintLatencyToInterval == 0 {
		return
	}
	ticker := time.NewTicker(nm.conf.PrintLatencyToInterval)
	for {
		<-ticker.C
		nm.watching.Range(
			func(k, v interface{}) bool {
				watchedPeer := v.(nodeWatchingValue)
				select {
				case <-watchedPeer.enoughSamples:
					nm.logger.Infof(
						"Node %s:, PHI: %f, Latency: %d",
						watchedPeer.Peer().String(),
						watchedPeer.Detector().Phi(),
						watchedPeer.LatencyCalc().CurrValue(),
					)
				default:
				}
				return true
			},
		)
	}
}
