package message

import (
	"encoding/binary"
	"errors"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
)

var ErrNotEnoughLen = errors.New("not enough length to deserialize")
var appMessageWrapperSerializer = AppMessageWrapperSerializer{}

type AppMessageWrapper struct {
	Sender            peer.Peer
	MessageID         message.ID
	SourceProto       protocol.ID
	DestProto         protocol.ID
	LenWrappedBytes   uint32
	WrappedMsgBytes   []byte
	TotalMessageBytes int
}

func NewAppMessageWrapper(
	mID message.ID,
	sourceProto protocol.ID,
	destProto protocol.ID,
	sender peer.Peer,
	wrappedMsgBytes []byte,
) *AppMessageWrapper {
	return &AppMessageWrapper{
		Sender:            sender,
		MessageID:         mID,
		SourceProto:       sourceProto,
		DestProto:         destProto,
		LenWrappedBytes:   uint32(len(wrappedMsgBytes)),
		WrappedMsgBytes:   wrappedMsgBytes,
		TotalMessageBytes: len(wrappedMsgBytes) + 18,
	}
}

// serializer

type AppMessageWrapperSerializer struct{}

func (wrapperMsg *AppMessageWrapper) Serialize() []byte {
	buf := make([]byte, 10)
	binary.BigEndian.PutUint16(buf, wrapperMsg.MessageID)
	binary.BigEndian.PutUint16(buf[2:], wrapperMsg.SourceProto)
	binary.BigEndian.PutUint16(buf[4:], wrapperMsg.DestProto)
	binary.BigEndian.PutUint32(buf[6:], uint32(len(wrapperMsg.WrappedMsgBytes)))
	buf = append(buf, wrapperMsg.Sender.Marshal()...)
	return append(buf, wrapperMsg.WrappedMsgBytes...)
}

func (wrapperMsg *AppMessageWrapper) Deserialize(buf []byte) (curr int, err error) {
	if len(buf) < 18 {
		return 0, ErrNotEnoughLen
	}
	curr = 0
	wrapperMsg.MessageID = binary.BigEndian.Uint16(buf)
	curr += 2
	wrapperMsg.SourceProto = binary.BigEndian.Uint16(buf[curr:])
	curr += 2
	wrapperMsg.DestProto = binary.BigEndian.Uint16(buf[curr:])
	curr += 2
	wrapperMsg.LenWrappedBytes = binary.BigEndian.Uint32(buf[curr : curr+4])
	curr += 4
	p := &peer.IPeer{}
	curr += p.Unmarshal(buf[curr:])
	if curr+int(wrapperMsg.LenWrappedBytes) > len(buf) {
		return 0, ErrNotEnoughLen
	}
	wrapperMsg.WrappedMsgBytes = buf[curr : curr+int(wrapperMsg.LenWrappedBytes)]
	wrapperMsg.Sender = p
	curr += int(wrapperMsg.LenWrappedBytes)
	wrapperMsg.TotalMessageBytes = curr
	return curr, nil
}
