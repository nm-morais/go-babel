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

func serializeHeartbeatMessage(m heartbeatMessage) []byte {
	if m.IsTest {
		return append([]byte{1}, m.sender.SerializeToBinary()...)
	}
	peerBytes := append([]byte{0}, m.sender.SerializeToBinary()...)

	var remainingBytes = peerBytes

	if m.IsReply {
		remainingBytes = append(remainingBytes, byte(0))
	} else {
		remainingBytes = append(remainingBytes, byte(1))
	}

	if m.ForceReply {
		remainingBytes = append(remainingBytes, byte(0))
	} else {
		remainingBytes = append(remainingBytes, byte(1))
	}

	tsBytes, err := m.TimeStamp.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return append(remainingBytes, tsBytes...)
}

func deserializeHeartbeatMessage(bytes []byte) heartbeatMessage {
	currPos := 0
	if bytes[0] == 1 {
		_, p := peer.DeserializePeer(bytes[1:])
		return heartbeatMessage{
			IsTest: true,
			sender: p,
		}
	}
	currPos++
	peerSize, p := peer.DeserializePeer(bytes[1:])
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

	return heartbeatMessage{
		TimeStamp:  ts,
		IsReply:    isReply,
		ForceReply: forceReply,
		IsTest:     false,
		sender:     p,
	}
}
