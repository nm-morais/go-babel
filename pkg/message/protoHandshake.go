package message

import (
	"encoding/binary"
	"github.com/nm-morais/go-babel/pkg/protocol"
	log "github.com/sirupsen/logrus"
	"net"
)

var protoHandshakeMessageSerializer = AppMessageWrapperSerializer{}

type ProtoHandshakeMessage struct {
	TemporaryConn uint8
	ListenAddr    net.Addr
	Protos        []protocol.ID
}

func NewProtoHandshakeMessage(protos []protocol.ID, listenAddr net.Addr, temporaryConn uint8) Message {
	return ProtoHandshakeMessage{
		ListenAddr:    listenAddr,
		Protos:        protos,
		TemporaryConn: temporaryConn,
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
	msgSize := 2*len(protoMsg.Protos) + 3
	buf := make([]byte, msgSize)
	bufPos := 0
	buf[0] = protoMsg.TemporaryConn
	bufPos++
	binary.BigEndian.PutUint16(buf[bufPos:], uint16(len(protoMsg.Protos)))
	bufPos += 2
	for _, protoID := range protoMsg.Protos {
		binary.BigEndian.PutUint16(buf[bufPos:], protoID)
		bufPos += 2
	}
	return append(buf, []byte(protoMsg.ListenAddr.String())...)
}

func (msg ProtoHandshakeMessageSerializer) Deserialize(buf []byte) Message {
	newMsg := &ProtoHandshakeMessage{}
	bufPos := 0
	newMsg.TemporaryConn = buf[0]
	bufPos++
	nrProtos := binary.BigEndian.Uint16(buf[bufPos:])
	bufPos += 2
	newMsg.Protos = make([]protocol.ID, nrProtos)
	for i := 0; uint16(i) < nrProtos; i++ {
		newMsg.Protos[i] = binary.BigEndian.Uint16(buf[bufPos:])
		bufPos += 2
	}
	listenAddrStr := string(buf[bufPos:])
	listenAddr, err := net.ResolveTCPAddr("tcp4", listenAddrStr)
	if err != nil {
		log.Errorf("Message %s has invalid format", string(buf))
		panic("Peer has invalid listen addr")
	}
	newMsg.ListenAddr = listenAddr
	return *newMsg
}
