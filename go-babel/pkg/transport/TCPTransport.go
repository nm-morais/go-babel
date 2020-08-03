package transport

import (
	"bytes"
	"github.com/DeMMon/go-babel/pkg"
	"github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	"github.com/DeMMon/go-babel/pkg/utils"
	log "github.com/sirupsen/logrus"
	"net"
)

const TCPTransportCaller = "TCPTransportCaller"

type TCPTransport struct {
	ListenAddr net.Addr
	conn       net.Conn
	peer       peer.Peer
	msgChan    chan message.Message
}

func GetTCPListener(listenAddr net.Addr) Transport {
	return &TCPTransport{
		ListenAddr: listenAddr,
		conn:       nil,
		peer:       nil,
	}
}

func GetTCPDialer() Transport {
	return &TCPTransport{
		ListenAddr: nil,
		conn:       nil,
		peer:       nil,
	}
}

func (t *TCPTransport) Listen() <-chan Transport {
	newConnChan := make(chan Transport)
	l, err := net.Listen("tcp4", t.ListenAddr.String())
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			panic(err)
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Error(err)
		}
		newTransport := &TCPTransport{
			ListenAddr: nil,
			conn:       conn,
			peer:       peer.NewPeer(conn.RemoteAddr()),
		}
		newConnChan <- newTransport
	}
}

func (t *TCPTransport) SendMessage(msg message.Message) pkg.Error {
	buf := bytes.NewBuffer(make([]byte, message.MaxMessageBytes))
	msg.Serialize(buf)
	_, err := t.conn.Write(buf.Bytes())
	if err != nil {
		return utils.NonFatalError(500, err.Error(), TCPTransportCaller)
	}
	return nil
}

func (t *TCPTransport) Dial(peer peer.Peer) <-chan pkg.Error {
	errChan := make(chan pkg.Error)
	go func() {
		addr, err := net.ResolveTCPAddr(peer.Addr().String(), peer.Addr().Network())
		if err != nil {
			errChan <- utils.NonFatalError(500, err.Error(), TCPTransportCaller)
			close(errChan)
			return
		}
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			errChan <- utils.NonFatalError(500, err.Error(), TCPTransportCaller)
			close(errChan)
			return
		}
		t.conn = conn
		errChan <- nil
		close(errChan)
		return
	}()

	return errChan
}

func (t *TCPTransport) PipeToMessageChan() <-chan message.Message {
	msgChan := make(chan message.Message)
	t.msgChan = msgChan
	go func() {
		for {
			msgBytes := make([]byte, message.MaxMessageBytes)
			read, err := t.conn.Read(msgBytes)
			if err != nil {
				log.Error(err)
				close(msgChan)
				return
			}
			msg := &message.GenericMessage{}
			msg.Deserialize(bytes.NewBuffer(msgBytes[:read]))
			msgChan <- msg
		}
	}()
	return msgChan
}

func (t *TCPTransport) Peer() peer.Peer {
	return t.peer
}

func (t *TCPTransport) MessageChan() <-chan message.Message {
	return t.msgChan
}

func (t *TCPTransport) Close() {
	if err := t.conn.Close(); err != nil { // TODO close nicely
		log.Error(err)
	}
}
