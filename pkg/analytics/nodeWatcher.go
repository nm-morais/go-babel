package analytics

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/errors"
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
	Latency     time.Duration
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
		logger:       logrus.New(),
	}
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
		Latency:     time.Duration(0),
		Detector:    NewDetector(nm.conf.WindowSize, nm.conf.MinSamplesFaultDetector),
		mr:          messageIO.NewMessageReader(stream),
		mw:          messageIO.NewMessageWriter(stream),
	}
	nm.watchingLock.Unlock()
	tcpStream, ok := stream.(*net.TCPConn)
	if ok {
		go nm.handleTCPConnection(tcpStream)
	}

	return nil
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

	testMsg := heartbeatMessage{sender: nm.selfPeer, IsTest: true}
	udpTestDeadline := time.Now().Add(nm.conf.UdpTestTimeout)
	udpConn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())})
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}

	for i := 0; i < nm.conf.NrTestMessages; i++ {
		mw := messageIO.NewMessageWriter(udpConn)
		_, err := mw.Write(serializeHeartbeatMessage(testMsg))
		if err != nil {
			break
		}
		udpConn.SetReadDeadline(udpTestDeadline)
		receiveBuf := make([]byte, 100)
		_, err = udpConn.Read(receiveBuf)
		if err != nil {
			break
		}
		// managed to read a message using tcp
		return udpConn, nil
	}

	// attempting to connect by TCP
	rAddr := &net.TCPAddr{IP: p.IP(), Port: int(p.AnalyticsPort())}
	tcpConn, err := net.DialTimeout("tcp", rAddr.String(), nm.conf.TcpTestTimeout)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), nodeWatcherCaller)
	}
	return tcpConn, nil
}

func (nm *NodeManagerImpl) start() {

	go nm.startTCPServer()
	go nm.startUDPServer()

	ticker := time.NewTicker(nm.conf.HbTickDuration)
	for {
		<-ticker.C
		nm.logger.Info("New nodeManager hb tick")
		nm.watchingLock.RLock()
		for _, nodeInfo := range nm.watching {
			_, err := nodeInfo.mw.Write(serializeHeartbeatMessage(NewHBMessageForceReply(nm.selfPeer)))
			if err != nil {
				go nm.attemptRepairStreamTo(nodeInfo.peer)
			}
		}
		nm.watchingLock.RUnlock()
	}
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
		hb := deserializeHeartbeatMessage(msgBuf[:n])
		go nm.handleHBMessageUDP(hb, udpConn, rAddr)
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
		go nm.handleTCPConnection(newStream)
	}
}

func (nm *NodeManagerImpl) handleTCPConnection(inConn *net.TCPConn) {
	mr := messageIO.NewMessageReader(inConn)
	for {
		msgBuf := make([]byte, 2048)
		n, rErr := mr.Read(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
		}
		hb := deserializeHeartbeatMessage(msgBuf[:n])
		go nm.handleHBMessageTCP(hb, inConn)
	}

}

func (nm *NodeManagerImpl) handleHBMessageTCP(hb heartbeatMessage, conn *net.TCPConn) errors.Error {
	mw := messageIO.NewMessageWriter(conn)
	if hb.IsTest {
		nm.logger.Infof("handling hb test message")
		_, err := mw.Write(serializeHeartbeatMessage(hb))
		if err != nil {
			nm.logger.Error(err)
		}
		return nil
	}
	if hb.IsReply {
		nm.logger.Infof("handling hb reply message")
		nm.registerHBReply(hb)
		return nil
	}
	if hb.ForceReply {
		nm.logger.Infof("replying to hb message")
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

func (nm *NodeManagerImpl) handleHBMessageUDP(hb heartbeatMessage, udpConn *net.UDPConn, rAddr *net.UDPAddr) errors.Error {
	if hb.IsTest {
		nm.logger.Infof("handling hb test message, sending it to %s", rAddr.String())
		_, err := udpConn.WriteTo(serializeHeartbeatMessage(hb), rAddr)
		if err != nil {
			nm.logger.Error(err)
		}
		return nil
	}
	if hb.IsReply {
		nm.logger.Infof("handling hb reply message")
		nm.registerHBReply(hb)
		return nil
	}
	if hb.ForceReply {
		nm.logger.Infof("replying to hb message")
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
	nm.watchingLock.RLock()
	defer nm.watchingLock.RUnlock()
	nodeInfo, ok := nm.watching[sender.ToString()]
	if !ok {
		nm.logger.Warn("Received reply for unwatched node, discarding...")
		return
	}
	nodeInfo.Detector.Ping(timeReceived)
	nodeInfo.Latency = time.Duration(float32(nodeInfo.Latency.Nanoseconds())*nm.conf.OldLatencyWeight + float32(timeTaken.Nanoseconds())*nm.conf.NewLatencyWeight)
	nm.watching[sender.ToString()] = nodeInfo
}
