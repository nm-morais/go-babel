package analytics

import (
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

type HeartbeatMessage struct {
	TimeStamp  time.Time
	IsReply    bool
	ForceReply bool
	Initial    bool
	Sender     peer.Peer
}

func NewHBMessageNoReply(sender peer.Peer, initial bool) HeartbeatMessage {
	return HeartbeatMessage{
		Sender:     sender,
		ForceReply: false,
		IsReply:    false,
		TimeStamp:  time.Now(),
		Initial:    initial,
	}
}

func NewHBMessageForceReply(sender peer.Peer, initial bool) HeartbeatMessage {
	return HeartbeatMessage{
		Sender:     sender,
		ForceReply: true,
		IsReply:    false,
		TimeStamp:  time.Now(),
		Initial:    initial,
	}
}

func SerializeHeartbeatMessage(m HeartbeatMessage) []byte {
	peerBytes := m.Sender.Marshal()

	if m.IsReply {
		peerBytes = append(peerBytes, byte(1))
	} else {
		peerBytes = append(peerBytes, byte(0))
	}

	if m.ForceReply {
		peerBytes = append(peerBytes, byte(1))
	} else {
		peerBytes = append(peerBytes, byte(0))
	}

	if m.Initial {
		peerBytes = append(peerBytes, byte(1))
	} else {
		peerBytes = append(peerBytes, byte(0))
	}

	tsBytes, err := m.TimeStamp.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return append(peerBytes, tsBytes...)
}

func DeserializeHeartbeatMessage(bytes []byte) HeartbeatMessage {
	currPos := 0
	p := &peer.IPeer{}
	currPos += p.Unmarshal(bytes[currPos:])
	isReply := bytes[currPos] == 1
	currPos++
	forceReply := bytes[currPos] == 1
	currPos++
	initial := bytes[currPos] == 1
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
		Sender:     p,
		Initial:    initial,
	}
	return hbMsg
}
