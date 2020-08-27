package analytics

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

type HeartbeatMessage struct {
	TimeStamp  time.Time
	IsReply    bool
	ForceReply bool
	IsTest     bool
	Sender     peer.Peer
}

func NewHBMessageForceReply(sender peer.Peer) HeartbeatMessage {
	return HeartbeatMessage{
		ForceReply: true,
		Sender:     sender,
		IsReply:    false,
		IsTest:     false,
		TimeStamp:  time.Now(),
	}
}

func NewTestHBMessage(sender peer.Peer) HeartbeatMessage {
	return HeartbeatMessage{
		Sender: sender,
		IsTest: true,
	}
}

func SerializeHeartbeatMessage(m HeartbeatMessage) []byte {
	if m.IsTest {
		return append([]byte{1}, m.Sender.SerializeToBinary()...)
	}
	peerBytes := append([]byte{0}, m.Sender.SerializeToBinary()...)

	var remainingBytes = peerBytes

	if m.IsReply {
		remainingBytes = append(remainingBytes, byte(1))
	} else {
		remainingBytes = append(remainingBytes, byte(0))
	}

	if m.ForceReply {
		remainingBytes = append(remainingBytes, byte(1))
	} else {
		remainingBytes = append(remainingBytes, byte(0))
	}

	tsBytes, err := m.TimeStamp.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return append(remainingBytes, tsBytes...)
}

func DeserializeHeartbeatMessage(bytes []byte) HeartbeatMessage {
	currPos := 1
	if bytes[0] == 1 { // isTest
		_, p := peer.DeserializePeer(bytes[currPos:])
		return HeartbeatMessage{
			IsTest: true,
			Sender: p,
		}
	}
	peerSize, p := peer.DeserializePeer(bytes[currPos:])
	currPos += peerSize

	isReply := bytes[currPos] == 1
	currPos++

	forceReply := bytes[currPos] == 1
	currPos++

	var ts time.Time
	err := ts.UnmarshalBinary(bytes[currPos:])
	if err != nil {
		panic(err)
	}
	hbMsg := HeartbeatMessage{
		TimeStamp:  ts,
		IsReply:    isReply,
		ForceReply: forceReply,
		IsTest:     false,
		Sender:     p,
	}
	return hbMsg
}
