package nodeWatcher

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/analytics"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/sirupsen/logrus"
)

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

type NodeInfo interface {
	Peer() peer.Peer
	LatencyCalc() *analytics.LatencyCalculator
	Detector() *analytics.PhiAccuralFailureDetector
}

type NodeWatcher interface {
	Watch(Peer peer.Peer, protoID protocol.ID) errors.Error
	WatchWithInitialLatencyValue(p peer.Peer, issuerProto protocol.ID, latency time.Duration) errors.Error
	Unwatch(peer peer.Peer, protoID protocol.ID) errors.Error
	GetNodeInfo(peer peer.Peer) (NodeInfo, errors.Error)
	GetNodeInfoWithDeadline(peer peer.Peer, deadline time.Time) (NodeInfo, errors.Error)
	NotifyOnCondition(c Condition) (int, errors.Error)
	CancelCond(condID int) errors.Error
	Logger() *logrus.Logger
}
