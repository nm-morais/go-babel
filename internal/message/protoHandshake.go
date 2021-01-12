package message

import (
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
)

var protoHandshakeMessageSerializer = AppMessageWrapperSerializer{}

type ProtoHandshakeMessage struct {
	Peer        peer.Peer
	DialerProto protocol.ID
	TunnelType  uint8
}

func NewProtoHandshakeMessage(dialerProto protocol.ID, peer peer.Peer, temporaryConn uint8) message.Message {
	return ProtoHandshakeMessage{
		DialerProto: dialerProto,
		Peer:        peer,
		TunnelType:  temporaryConn,
	}
}

func (msg ProtoHandshakeMessage) Type() message.ID {
	panic("implement me")
}

type ProtoHandshakeMessageSerializer struct{}

func (msg ProtoHandshakeMessage) Serializer() message.Serializer {
	return protoHandshakeMessageSerializer
}

func (msg ProtoHandshakeMessage) Deserializer() message.Deserializer {
	return protoHandshakeMessageSerializer
}

func (msg ProtoHandshakeMessageSerializer) Serialize(message message.Message) []byte {
	protoMsg := message.(ProtoHandshakeMessage)
	msgSize := 3
	buf := make([]byte, msgSize)
	bufPos := 0
	buf[0] = protoMsg.TunnelType
	bufPos++
	binary.BigEndian.PutUint16(buf[bufPos:], protoMsg.DialerProto)
	toSend := append(buf, protoMsg.Peer.Marshal()...)
	return toSend
}

func (msg ProtoHandshakeMessageSerializer) Deserialize(buf []byte) message.Message {
	newMsg := ProtoHandshakeMessage{}
	bufPos := 0
	newMsg.TunnelType = buf[0]
	bufPos++
	dialerProto := binary.BigEndian.Uint16(buf[bufPos:])
	bufPos += 2
	p := &peer.IPeer{}
	p.Unmarshal(buf[bufPos:])
	newMsg.Peer = p
	newMsg.DialerProto = dialerProto
	return newMsg
}
