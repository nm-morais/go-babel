package analytics

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

type heartbeatMessage struct {
	TimeStamp  time.Time
	IsReply    bool
	ForceReply bool
	IsTest     bool
	sender     peer.Peer
}

func NewHBMessageForceReply(sender peer.Peer) heartbeatMessage {
	return heartbeatMessage{
		ForceReply: true,
		sender:     sender,
		IsReply:    false,
		IsTest:     false,
		TimeStamp:  time.Now(),
	}
}

func NewTestHBMessage(sender peer.Peer) heartbeatMessage {
	return heartbeatMessage{
		sender: sender,
		IsTest: true,
	}
}

func serializeHeartbeatMessage(m heartbeatMessage) []byte {
	if m.IsTest {
		return append([]byte{1}, m.sender.SerializeToBinary()...)
	}
	peerBytes := append([]byte{0}, m.sender.SerializeToBinary()...)

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

func deserializeHeartbeatMessage(bytes []byte) heartbeatMessage {
	currPos := 1
	if bytes[0] == 1 { // isTest
		_, p := peer.DeserializePeer(bytes[currPos:])
		return heartbeatMessage{
			IsTest: true,
			sender: p,
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
	hbMsg := heartbeatMessage{
		TimeStamp:  ts,
		IsReply:    isReply,
		ForceReply: forceReply,
		IsTest:     false,
		sender:     p,
	}
	return hbMsg
}
