package message

import (
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"net"
)

var protoHandshakeMessageSerializer = AppMessageWrapperSerializer{}

type ProtoHandshakeMessage struct {
	ListenAddr net.Addr
	Protos     []protocol.ID
}

func NewProtoHandshakeMessage(protos []protocol.ID, listenAddr net.Addr) Message {
	return ProtoHandshakeMessage{
		ListenAddr: listenAddr,
		Protos:     protos,
	}
}

func (msg ProtoHandshakeMessage) Type() ID {
	panic("implement me")
}

type ProtoHandshakeMessageSerializer struct{}

func (msg ProtoHandshakeMessage) Serializer() Serializer {
	return protoHandshakeMessageSerializer
}

func (msg ProtoHandshakeMessage) Deserializer() Deserializer {
	return protoHandshakeMessageSerializer
}

func (msg ProtoHandshakeMessageSerializer) Serialize(message Message) []byte {
	protoMsg := message.(ProtoHandshakeMessage)
	msgSize := 2*len(protoMsg.Protos) + 2
	buf := make([]byte, msgSize)
	binary.BigEndian.PutUint16(buf, uint16(len(protoMsg.Protos)))
	for i, protoID := range protoMsg.Protos {
		bufPos := i*2 + 2
		binary.BigEndian.PutUint16(buf[bufPos:], uint16(protoID))
	}
	return append(buf, []byte(protoMsg.ListenAddr.String())...)

}

func (msg ProtoHandshakeMessageSerializer) Deserialize(buf []byte) Message {
	newMsg := &ProtoHandshakeMessage{}
	nrProtos := binary.BigEndian.Uint16(buf)
	newMsg.Protos = make([]protocol.ID, nrProtos)
	bufPos := 0
	for i := 0; uint16(i) < nrProtos; i++ {
		bufPos = i*2 + 2
		newMsg.Protos[i] = protocol.ID(binary.BigEndian.Uint16(buf[bufPos:]))
	}
	bufPos += 2
	listenAddrStr := string(buf[bufPos:])
	listenAddr, err := net.ResolveTCPAddr("tcp4", listenAddrStr)
	if err != nil {
		panic("Peer has invalid listen addr")
	}
	newMsg.ListenAddr = listenAddr
	return *newMsg
}
