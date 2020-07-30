package connection

import (
	"encoding/json"
	"fmt"
	. "github.com/DeMMon/go-babel/internal/connection"
	. "github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/protocol"
	"github.com/DeMMon/go-babel/pkg/utils"
	log "github.com/sirupsen/logrus"
	"net"
)

const caller = "TCPConnection"

type TCPConnection struct {
	protocol protocol.Protocol
	peer     peer.Peer
	conn     net.Conn
}

func NewTCPConnection(protocol protocol.Protocol, peer peer.Peer) BidirectionalConnection {
	return &TCPConnection{
		protocol: protocol,
		peer:     peer,
	}
}

func (d TCPConnection) Protocol() protocol.Protocol {
	return d.protocol
}

func (d TCPConnection) SendMessage(message message.Message) Error {
	_, err := d.conn.Write(message.Serialize())
	if err != nil {
		return utils.NonFatalError(500, err.Error(), caller)
	}
	return nil
}

func (d TCPConnection) ConnectTo(peer peer.Peer) Error {
	addr := peer.Addr()
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr.String())
	if err != nil {
		reason := "ResolveTCPAddr failed:" + err.Error()
		return utils.NonFatalError(500, reason, caller)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		reason := "Dial failed:" + err.Error()
		return utils.NonFatalError(500, reason, caller)
	}

	d.protocol.OutConnEstablished(peer)
	d.HandleConn(conn)
	return nil
}

func (d TCPConnection) HandleConn(c net.Conn) {
	log.Infof("New peer: %s\n", c.RemoteAddr().String())
	decoder := json.NewDecoder(c)
	defer func() {
		if err := c.Close(); err != nil {
			log.Error("Connection error:  %+v", err)
		}
	}()
	for {
		var msgWrapper message.Wrapper
		err := decoder.Decode(&msgWrapper)
		fmt.Println(msgWrapper, err)
	}
}

func (d TCPConnection) waitForMetadataMessage() (*ConnMetadataMessage, Error) {
	decoder := json.NewDecoder(d.conn)
	var metadataMsg ConnMetadataMessage
	err := decoder.Decode(&metadataMsg)
	if err != nil {
		return nil, utils.FatalError(500, err.Error(), caller)
	}
	return &metadataMsg, nil
}

func (d TCPConnection) sendMetadataMessage() (*ConnMetadataMessage, Error) {
	msgToSend := NewConnMetadataMessage()
	d.SendMessage()
}
