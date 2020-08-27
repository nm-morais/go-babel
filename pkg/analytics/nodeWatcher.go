package analytics

import (
	"context"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

const nodeWatcherCaller = "NodeWatcher"

type NodeInfo struct {
	mw          io.Writer
	mr          io.Reader
	nrRetries   int
	subscribers map[protocol.ID]bool
	peer        peer.Peer
	peerConn    net.Conn
	LatencyCalc *LatencyCalculator
	Detector    *Detector
}

type NodeManagerImpl struct {
	selfPeer     peer.Peer
	conf         NodeWatcherConf
	dialing      map[string]bool
	dialingLock  *sync.RWMutex
	watching     map[string]NodeInfo
	watchingLock *sync.RWMutex
	logger       *logrus.Logger
}

type NodeWatcherConf struct {
	MaxRedials              int
	TcpTestTimeout          time.Duration
	UdpTestTimeout          time.Duration
	NrTestMessages          int
	HbTickDuration          time.Duration
	WindowSize              int
	MinSamplesFaultDetector int
	OldLatencyWeight        float32
	NewLatencyWeight        float32
}

type NodeWatcher interface {
	Watch(Peer peer.Peer, protoID protocol.ID) errors.Error
	Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error
	GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error)
	GetNodeInfoWithContext(peer peer.Peer, ctx context.Context) (*NodeInfo, errors.Error)
	NotifyOnCondition(p peer.Peer, notification notification.Notification, protoID protocol.ID) errors.Error
	Logger() *logrus.Logger
}

func NewNodeWatcher(selfPeer peer.Peer, config NodeWatcherConf) NodeWatcher {
	nm := &NodeManagerImpl{
		conf:         config,
		selfPeer:     selfPeer,
		watching:     make(map[string]NodeInfo),
		watchingLock: &sync.RWMutex{},
		dialing:      make(map[string]bool),
		dialingLock:  &sync.RWMutex{},
		logger:       logs.NewLogger(nodeWatcherCaller),
	}
	nm.logger.Infof("My configs: %+v", config)
	go nm.start()
	return nm
}

func (nm *NodeManagerImpl) Watch(p peer.Peer, protoID protocol.ID) errors.Error {
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
		subscribers: map[protocol.ID]bool{protoID: true},
		peerConn:    stream,
		peer:        p,
		LatencyCalc: NewLatencyCalculator(nm.conf.NewLatencyWeight, nm.conf.OldLatencyWeight),
		Detector:    NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
		mr:          messageIO.NewMessageReader(stream),
		mw:          messageIO.NewMessageWriter(stream),
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

func (nm *NodeManagerImpl) startHbRoutine(p peer.Peer) {
	ticker := time.NewTicker(nm.conf.HbTickDuration)
	for {
		<-ticker.C
		nm.watchingLock.RLock()
		nodeInfo, ok := nm.watching[p.ToString()]
		if !ok {
			return
		}
		nm.watchingLock.RUnlock()
		nm.logger.Infof("Node %s: ConnType: %s PHI: %f, Latency: %d, Subscribers: %d ", nodeInfo.peer.ToString(), reflect.TypeOf(nodeInfo.peerConn), nodeInfo.Detector.Phi(time.Now()), nodeInfo.LatencyCalc.GetCurrMeasurement(), len(nodeInfo.subscribers))
		toSend := NewHBMessageForceReply(nm.selfPeer)
		switch nodeInfo.peerConn.(type) {
		case net.PacketConn:
			//nm.logger.Infof("sending hb message:%+v", toSend)
			nm.logger.Infof("Sent HB to %s via UDP", nodeInfo.peer.ToString())
			_, err := nodeInfo.peerConn.Write(serializeHeartbeatMessage(toSend))
			if err != nil {
				nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		case net.Conn:
			nm.logger.Infof("Sent HB to %s via TCP", nodeInfo.peer.ToString())
			_, err := nodeInfo.mw.Write(serializeHeartbeatMessage(NewHBMessageForceReply(nm.selfPeer)))
			if err != nil {
				nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		}
	}
}

func (nm *NodeManagerImpl) Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error {
	nm.watchingLock.Lock()
	defer nm.watchingLock.Unlock()
	if _, ok := nm.watching[peer.ToString()]; ok {
		return errors.NonFatalError(404, "peer not being tracked", nodeWatcherCaller)
	}
	delete(nm.watching, peer.ToString())
	return nil
}

func (nm *NodeManagerImpl) GetNodeInfo(peer peer.Peer) (*NodeInfo, errors.Error) {
	panic("not implemented") // TODO: Implement
}

func (nm *NodeManagerImpl) GetNodeInfoWithContext(peer peer.Peer, ctx context.Context) (*NodeInfo, errors.Error) {
	panic("not implemented") // TODO: Implement
}

func (nm *NodeManagerImpl) NotifyOnCondition(p peer.Peer, notification notification.Notification, protoID protocol.ID) errors.Error {
	panic("not implemented") // TODO: Implement
}

func (nm *NodeManagerImpl) Logger() *logrus.Logger {
	return nm.logger
}

func (nm *NodeManagerImpl) establishStreamTo(p peer.Peer) (net.Conn, errors.Error) {

	//nm.logger.Infof("Establishing stream to %s", p.ToString())
	testMsg := heartbeatMessage{sender: nm.selfPeer, IsTest: true, IsReply: false, ForceReply: false}
	udpTestDeadline := time.Now().Add(nm.conf.UdpTestTimeout)
	rAddrUdp := &net.UDPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	udpConn, err := net.DialUDP("udp", nil, rAddrUdp)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}

	for i := 0; i < nm.conf.NrTestMessages; i++ {
		//nm.logger.Infof("Writing test message to: %s", p.ToString())
		_, err := udpConn.Write(serializeHeartbeatMessage(testMsg))
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

	// attempting to connect by TCP
	rAddrTcp := &net.TCPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	tcpConn, err := net.DialTimeout("tcp", rAddrTcp.String(), nm.conf.TcpTestTimeout)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}
	return tcpConn, nil
}

func (nm *NodeManagerImpl) start() {
	go nm.startTCPServer()
	go nm.startUDPServer()
}

func (nm *NodeManagerImpl) attemptRepairStreamTo(p peer.Peer) {
	stream, err := nm.establishStreamTo(p)
	nm.watchingLock.Lock()
	currStatus := nm.watching[p.ToString()]
	if err != nil {
		currStatus.nrRetries++
		if currStatus.nrRetries >= nm.conf.MaxRedials {
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
	nm.watchingLock.Unlock()
}

func (nm *NodeManagerImpl) startUDPServer() {
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

func (nm *NodeManagerImpl) startTCPServer() {
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

func (nm *NodeManagerImpl) handleTCPConnection(inConn *net.TCPConn) {
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

func (nm *NodeManagerImpl) handleUDPConnection(inConn *net.UDPConn) {
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

func (nm *NodeManagerImpl) handleHBMessageTCP(hbBytes []byte, mw io.Writer) errors.Error {
	hb := deserializeHeartbeatMessage(hbBytes)
	if hb.IsTest {
		//nm.logger.Infof("handling hb test message %+v", hb)
		_, err := mw.Write(serializeHeartbeatMessage(hb))
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
		toSend := heartbeatMessage{
			TimeStamp:  hb.TimeStamp,
			sender:     nm.selfPeer,
			IsReply:    true,
			ForceReply: false,
			IsTest:     false,
		}
		_, err := mw.Write(serializeHeartbeatMessage(toSend))
		if err != nil {
			return errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
		}
		return nil
	}
	panic("Should not be here")
}

func (nm *NodeManagerImpl) handleHBMessageUDP(hbBytes []byte, udpConn *net.UDPConn, rAddr *net.UDPAddr) errors.Error {
	hb := deserializeHeartbeatMessage(hbBytes)
	//nm.logger.Infof("handling hb message:%+v", hb)
	if hb.IsTest {
		//nm.logger.Infof("handling hb test message, sending it to %s", rAddr.String())
		_, err := udpConn.WriteTo(serializeHeartbeatMessage(hb), rAddr)
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
		toSend := heartbeatMessage{
			TimeStamp:  hb.TimeStamp,
			sender:     nm.selfPeer,
			IsReply:    true,
			ForceReply: false,
			IsTest:     false,
		}

		_, err := udpConn.WriteTo(serializeHeartbeatMessage(toSend), rAddr)
		if err != nil {
			return errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
		}
		return nil
	}
	panic("Should not be here")
}

func (nm *NodeManagerImpl) registerHBReply(hb heartbeatMessage) {
	sender := hb.sender
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
	nm.watchingLock.Unlock()
}
