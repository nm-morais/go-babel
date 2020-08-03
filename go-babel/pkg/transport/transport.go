package transport

import (
	internalMessage "github.com/DeMMon/go-babel/internal/message"
	"github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/protocol"
)

type Transport interface {
	Listen() <-chan Transport
	Peer() peer.Peer
	Dial(peer peer.Peer) <-chan pkg.Error
	PipeToMessageChan() <-chan message.Message
	MessageChan() <-chan message.Message
	SendMessage(message message.Message) pkg.Error
	Close()
}

func ExchangeProtos(transport Transport, selfProtos []protocol.ID) []protocol.ID {
	var toSend = internalMessage.NewProtoHandshakeMessage(selfProtos)
	transport.SendMessage(toSend)
	msgChan := transport.MessageChan()
	msg := <-msgChan
	genericMsg := msg.(*message.GenericMessage)
	protoMsg := toSend.(*internalMessage.ProtoHandshakeMessage)
	protoMsg.Deserialize(genericMsg.MSgBytes)
	return protoMsg.Protos
}
