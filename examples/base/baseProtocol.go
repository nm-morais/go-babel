package baseProtocol

import (
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const name = "<replace-me>"

type BaseProtocol struct {
	logger *logrus.Logger
	babel  protocolManager.ProtocolManager
}

func NewBaseProtocol(babel protocolManager.ProtocolManager) protocol.Protocol {
	return &BaseProtocol{
		logger: logs.NewLogger(name),
		babel:  babel,
	}
}

func (m *BaseProtocol) MessageDelivered(message message.Message, peer peer.Peer) {
}

func (m *BaseProtocol) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
}

func (m *BaseProtocol) ID() protocol.ID {
	return protoID
}

func (m *BaseProtocol) Name() string {
	return name
}

func (m *BaseProtocol) Logger() *logrus.Logger {
	return m.logger
}

func (m *BaseProtocol) Init() {
}

func (m *BaseProtocol) Start() {
}

func (m *BaseProtocol) DialFailed(p peer.Peer) {
}

func (m *BaseProtocol) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	return sourceProto != m.ID()
}

func (m *BaseProtocol) InConnRequested(dialerProto protocol.ID, peer peer.Peer) bool {
	return dialerProto != m.ID()
}

func (m *BaseProtocol) OutConnDown(peer peer.Peer) {
}
