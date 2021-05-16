package message

import (
	"encoding/binary"
	"errors"

	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
)

var ErrNotEnoughLen = errors.New("not enough length to deserialize")

type AppMessageWrapper struct {
	Sender          peer.Peer
	MessageID       message.ID
	SourceProto     protocol.ID
	DestProto       protocol.ID
	WrappedMsgBytes []byte
}

func NewAppMessageWrapper(
	mID message.ID,
	sourceProto protocol.ID,
	destProto protocol.ID,
	sender peer.Peer,
	wrappedMsgBytes []byte,
) *AppMessageWrapper {
	return &AppMessageWrapper{
		Sender:          sender,
		MessageID:       mID,
		SourceProto:     sourceProto,
		DestProto:       destProto,
		WrappedMsgBytes: wrappedMsgBytes,
	}
}

// serializer

type AppMessageWrapperSerializer struct{}

func (wrapperMsg *AppMessageWrapper) Serialize() []byte {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf, wrapperMsg.MessageID)
	binary.BigEndian.PutUint16(buf[2:], wrapperMsg.SourceProto)
	binary.BigEndian.PutUint16(buf[4:], wrapperMsg.DestProto)
	buf = append(buf, wrapperMsg.Sender.Marshal()...)
	return append(buf, wrapperMsg.WrappedMsgBytes...)
}

// func (wrapperMsg *AppMessageWrapper) Deserialize(reader *bytes.Buffer) (int, error) {
// 	if reader.Len() < 14 {
// 		return 0, ErrNotEnoughLen
// 	}

// 	if err := binary.Read(reader, binary.BigEndian, &wrapperMsg.MessageID); err != nil {
// 		return 0, err
// 	}

// 	if err := binary.Read(reader, binary.BigEndian, &wrapperMsg.SourceProto); err != nil {
// 		return 0, err
// 	}

// 	if err := binary.Read(reader, binary.BigEndian, &wrapperMsg.DestProto); err != nil {
// 		return 0, err
// 	}

// 	peerBytes := make([]byte, 8)
// 	if _, err := reader.Read(peerBytes); err != nil {
// 		return 0, err
// 	}
// 	tmp := &peer.IPeer{}
// 	tmp.UnmarshalFromBuf(reader)
// 	wrapperMsg.Sender = tmp
// 	wrapperMsg.WrappedMsgBytes = reader.Bytes()
// 	return len(wrapperMsg.WrappedMsgBytes) + 14, nil
// }

func (wrapperMsg *AppMessageWrapper) Deserialize(buf []byte) (curr int, err error) {
	if len(buf) < 14 {
		return 0, ErrNotEnoughLen
	}
	curr = 0
	wrapperMsg.MessageID = binary.BigEndian.Uint16(buf)
	curr += 2
	wrapperMsg.SourceProto = binary.BigEndian.Uint16(buf[curr:])
	curr += 2
	wrapperMsg.DestProto = binary.BigEndian.Uint16(buf[curr:])
	curr += 2
	p := &peer.IPeer{}
	curr += p.Unmarshal(buf[curr:])
	wrapperMsg.WrappedMsgBytes = buf[curr:]
	wrapperMsg.Sender = p
	curr += len(wrapperMsg.WrappedMsgBytes)
	return curr, nil
}
