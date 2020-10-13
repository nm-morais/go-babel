package main

import (
	"encoding/binary"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
)

const JoinMessageType = 2000

type JoinMessage struct{}
type joinMessageSerializer struct{}

var defaultJoinMessageSerializer = joinMessageSerializer{}

func (JoinMessage) Type() message.ID                                      { return JoinMessageType }
func (JoinMessage) Serializer() message.Serializer                        { return defaultJoinMessageSerializer }
func (JoinMessage) Deserializer() message.Deserializer                    { return defaultJoinMessageSerializer }
func (joinMessageSerializer) Serialize(msg message.Message) []byte        { return []byte{} }
func (joinMessageSerializer) Deserialize(msgBytes []byte) message.Message { return JoinMessage{} }

const DisconnectMessageType = 2001

type DisconnectMessage struct{}
type disconnectMessageSerializer struct{}

var defaultDisconnectMessageSerializer = disconnectMessageSerializer{}

func (DisconnectMessage) Type() message.ID               { return DisconnectMessageType }
func (DisconnectMessage) Serializer() message.Serializer { return defaultDisconnectMessageSerializer }
func (DisconnectMessage) Deserializer() message.Deserializer {
	return defaultDisconnectMessageSerializer
}
func (disconnectMessageSerializer) Serialize(msg message.Message) []byte        { return []byte{} }
func (disconnectMessageSerializer) Deserialize(msgBytes []byte) message.Message { return JoinMessage{} }

const ForwardJoinMessageType = 2002

type ForwardJoinMessage struct {
	TTL            uint32
	OriginalSender peer.Peer
}
type forwardJoinMessageSerializer struct{}

var defaultForwardJoinMessageSerializer = forwardJoinMessageSerializer{}

func (ForwardJoinMessage) Type() message.ID               { return ForwardJoinMessageType }
func (ForwardJoinMessage) Serializer() message.Serializer { return defaultForwardJoinMessageSerializer }
func (ForwardJoinMessage) Deserializer() message.Deserializer {
	return defaultForwardJoinMessageSerializer
}
func (forwardJoinMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(ForwardJoinMessage)
	ttlBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ttlBytes, converted.TTL)
	return append(ttlBytes, converted.OriginalSender.Marshal()...)
}

func (forwardJoinMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	ttl := binary.BigEndian.Uint32(msgBytes[0:4])
	p := &peer.IPeer{}
	p.Unmarshal(msgBytes[4:])
	return ForwardJoinMessage{
		TTL:            ttl,
		OriginalSender: p,
	}
}

const NeighbourMessageType = 2003

type NeighbourMessage struct {
	HighPrio bool
}
type neighbourMessageSerializer struct{}

var defaultNeighbourMessageSerializer = neighbourMessageSerializer{}

func (NeighbourMessage) Type() message.ID               { return NeighbourMessageType }
func (NeighbourMessage) Serializer() message.Serializer { return defaultNeighbourMessageSerializer }
func (NeighbourMessage) Deserializer() message.Deserializer {
	return defaultNeighbourMessageSerializer
}
func (neighbourMessageSerializer) Serialize(msg message.Message) []byte {
	converted := msg.(NeighbourMessage)
	var msgBytes []byte
	if converted.HighPrio {
		msgBytes = []byte{1}
	} else {
		msgBytes = []byte{0}
	}
	return msgBytes
}

func (neighbourMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	highPrio := msgBytes[0] == 1
	return NeighbourMessage{
		HighPrio: highPrio,
	}
}

const NeighbourMessageReplyType = 2004

type NeighbourMessageReply struct {
	Accepted bool
}
type neighbourMessageReplySerializer struct{}

var defaultNeighbourMessageReplySerializer = neighbourMessageReplySerializer{}

func (NeighbourMessageReply) Type() message.ID { return NeighbourMessageReplyType }
func (NeighbourMessageReply) Serializer() message.Serializer {
	return defaultNeighbourMessageReplySerializer
}
func (NeighbourMessageReply) Deserializer() message.Deserializer {
	return defaultNeighbourMessageReplySerializer
}
func (neighbourMessageReplySerializer) Serialize(msg message.Message) []byte {
	converted := msg.(NeighbourMessageReply)
	var msgBytes []byte
	if converted.Accepted {
		msgBytes = []byte{1}
	} else {
		msgBytes = []byte{0}
	}
	return msgBytes
}

func (neighbourMessageReplySerializer) Deserialize(msgBytes []byte) message.Message {
	accepted := msgBytes[0] == 1
	return NeighbourMessageReply{
		Accepted: accepted,
	}
}

const ShuffleMessageType = 2005

type ShuffleMessage struct {
	ID    uint32
	TTL   uint32
	Peers []peer.Peer
}
type ShuffleMessageSerializer struct{}

var defaultShuffleMessageSerializer = ShuffleMessageSerializer{}

func (ShuffleMessage) Type() message.ID { return ShuffleMessageType }
func (ShuffleMessage) Serializer() message.Serializer {
	return defaultShuffleMessageSerializer
}
func (ShuffleMessage) Deserializer() message.Deserializer {
	return defaultShuffleMessageSerializer
}
func (ShuffleMessageSerializer) Serialize(msg message.Message) []byte {
	msgBytes := make([]byte, 12)
	shuffleMsg := msg.(ShuffleMessage)
	binary.BigEndian.PutUint32(msgBytes[0:4], shuffleMsg.ID)
	binary.BigEndian.PutUint32(msgBytes[4:8], shuffleMsg.TTL)
	peer.SerializePeerArray(shuffleMsg.Peers)
	return msgBytes
}

func (ShuffleMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	id := binary.BigEndian.Uint32(msgBytes[0:4])
	ttl := binary.BigEndian.Uint32(msgBytes[4:8])
	_, hosts := peer.DeserializePeerArray(msgBytes[8:])
	return ShuffleMessage{
		ID:    id,
		TTL:   ttl,
		Peers: hosts,
	}
}

const ShuffleReplyMessageType = 2006

type ShuffleReplyMessage struct {
	ID    uint32
	Peers []peer.Peer
}
type ShuffleReplyMessageSerializer struct{}

var defaultShuffleReplyMessageSerializer = ShuffleReplyMessageSerializer{}

func (ShuffleReplyMessage) Type() message.ID { return ShuffleReplyMessageType }
func (ShuffleReplyMessage) Serializer() message.Serializer {
	return defaultShuffleReplyMessageSerializer
}
func (ShuffleReplyMessage) Deserializer() message.Deserializer {
	return defaultShuffleReplyMessageSerializer
}
func (ShuffleReplyMessageSerializer) Serialize(msg message.Message) []byte {
	msgBytes := make([]byte, 8)
	shuffleMsg := msg.(ShuffleReplyMessage)
	binary.BigEndian.PutUint32(msgBytes[0:4], shuffleMsg.ID)
	return append(msgBytes, peer.SerializePeerArray(shuffleMsg.Peers)...)
}

func (ShuffleReplyMessageSerializer) Deserialize(msgBytes []byte) message.Message {
	id := binary.BigEndian.Uint32(msgBytes[0:4])
	_, hosts := peer.DeserializePeerArray(msgBytes[4:])
	return ShuffleReplyMessage{
		ID:    id,
		Peers: hosts,
	}
}
