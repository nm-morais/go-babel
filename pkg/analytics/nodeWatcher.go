package analytics

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/sirupsen/logrus"
)

const NodeWatcherCaller = "NodeWatcher"

type NodeInfo struct {
	Latency  time.Duration
	Phi      float32
	detector *Detector
}

type NodeManagerImpl struct {
	selfPeer     peer.Peer
	conf         NodeManagerConf
	watching     map[string]NodeInfo
	watchingLock *sync.RWMutex
	logger       *logrus.Logger
}

type NodeManagerConf struct {
	hbTickDuration   time.Duration
	windowSize       int
	minSamples       int
	oldLatencyWeight float32
	newLatencyWeight float32
}

type NodeManager interface {
	Watch(Peer peer.Peer, protoID protocol.ID) errors.Error
	Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error
	GetNodeInfo(peer peer.Peer) (*time.Duration, errors.Error)
	GetNodeInfoWithContext(peer peer.Peer, ctx context.Context) (*time.Duration, errors.Error)
	GetNodePhi(peer peer.Peer) errors.Error
	NotifyOnCondition(p peer.Peer, notification notification.Notification, protoID protocol.ID) errors.Error
	Logger() *logrus.Logger
}

func NewNodeManager(selfPeer peer.Peer, config NodeManagerConf) NodeManager {
	nm := &NodeManagerImpl{
		conf:         config,
		selfPeer:     selfPeer,
		watching:     make(map[string]NodeInfo),
		watchingLock: &sync.RWMutex{},
	}
	return nm
}

func (nm *NodeManagerImpl) Watch(p peer.Peer, protoID protocol.ID) errors.Error {
	nm.watchingLock.Lock()
	defer nm.watchingLock.Unlock()
	if _, ok := nm.watching[p.ToString()]; ok {
		return errors.NonFatalError(409, "peer already being tracked", NodeWatcherCaller)
	}
	nm.watching[p.ToString()] = NodeInfo{
		Latency:  time.Duration(0),
		detector: NewDetector(nm.conf.windowSize, nm.conf.minSamples),
	}

	return nil
}

func (nm *NodeManagerImpl) Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error {
	nm.watchingLock.Lock()
	defer nm.watchingLock.Unlock()
	if _, ok := nm.watching[peer.ToString()]; ok {
		return errors.NonFatalError(404, "peer not being tracked", NodeWatcherCaller)
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

func (nm *NodeManagerImpl) start() {

	go nm.startTCPServer()
	go nm.startUDPServer()
	ticker := time.NewTicker(nm.conf.hbTickDuration)
	for {
		<-ticker.C
		nm.logger.Info("New nodeManager hb tick")
		nm.watchingLock.Lock()

		nm.watchingLock.Unlock()
	}
}

func (nm *NodeManagerImpl) startUDPServer() {
	listenAddr := &net.UDPAddr{
		IP:   nm.selfPeer.IP(),
		Port: int(nm.selfPeer.ProtosPort()),
	}

	listener, err := stream.NewUDPListener(listenAddr).Listen()
	if err != nil {
		panic(err)
	}

	for {
		newStream, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		msgBuf := make([]byte, 67)
		n, rErr := newStream.Read(msgBuf)
		if rErr != nil {
			nm.logger.Warn(rErr)
			continue
		}
		hb := deserializeHeartbeatMessage(msgBuf[:n])
		go nm.handleHBMessageUDP(hb, newStream)
	}
}

func (nm *NodeManagerImpl) startTCPServer() {
	//tcpStream := stream.NewTCPListener()
	panic("not implemented")
}

func (nm *NodeManagerImpl) handleHBMessageUDP(hb heartbeatMessage, conn stream.Stream) {
	if hb.IsTest {
		nm.logger.Error("replying to test message")
		_, err := conn.Write(serializeHeartbeatMessage(hb))
		if err != nil {
			nm.logger.Error(err)
		}
		return
	}

	if hb.IsReply {
		nm.registerHBReply(hb)
		return
	}

}

func (nm *NodeManagerImpl) handleHBMessageTCP() {

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
	nodeInfo.detector.Ping(timeReceived)
	nodeInfo.Latency = time.Duration(float32(nodeInfo.Latency.Nanoseconds())*nm.conf.oldLatencyWeight + float32(timeTaken.Nanoseconds())*nm.conf.newLatencyWeight)
	nm.watching[sender.ToString()] = nodeInfo
}
