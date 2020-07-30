package connection

import (
	. "github.com/DeMMon/go-babel/internal/utils"
	"github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"net"
)

const msgID = "ConnMetadataMessage"

type ConnMetadataMessage struct {
	msgID      pkg.ID
	protocolID pkg.ID
	senderID   pkg.ID
	senderAddr net.Addr
}

func NewConnMetadataMessage(protocol protocol.Protocol, senderID pkg.ID) *ConnMetadataMessage {
	return &ConnMetadataMessage{
		msgID:      GenerateMessageID(),
		protocolID: protocol.ID(),
		senderID:   senderID,
	}
}

func (i ConnMetadataMessage) Type() pkg.MessageType {
	return msgID
}

func (i ConnMetadataMessage) ID() pkg.ID {
	return i.msgID
}

func (i ConnMetadataMessage) Serialize() []byte {
	return SerializeToJson(i)
}

func (i ConnMetadataMessage) Deserialize(msgBytes []byte) message.Message {
	deserialized := ConnMetadataMessage{}
	DeserializeFromJson(msgBytes, &deserialized)
	return deserialized
}
