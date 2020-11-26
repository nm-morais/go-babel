package pkg

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	internalMsg "github.com/nm-morais/go-babel/internal/message"
	"github.com/nm-morais/go-babel/internal/messageIO"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	log "github.com/sirupsen/logrus"
)

var (
	encoderConfig = messageIO.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}

	decoderConfig = messageIO.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
)

type StreamManagerConf struct {
	DialTimeout time.Duration
}

type babelStreamManager struct {
	conf StreamManagerConf

	babel                  protocolManager.ProtocolManager
	udpConn                *net.UDPConn
	outboundTransports     *sync.Map
	dialingTransportsMutex *sync.RWMutex
	inboundTransports      *sync.Map
	logger                 *log.Logger
}

type outboundTransport struct {
	Addr net.Addr

	Dialed   chan interface{}
	DialErr  chan interface{}
	Finished chan interface{}

	MsgChan chan []byte
}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

const streamManagerCaller = "StreamManager"

func NewStreamManager(babel protocolManager.ProtocolManager, conf StreamManagerConf) *babelStreamManager {
	sm := &babelStreamManager{
		conf:                   conf,
		babel:                  babel,
		udpConn:                &net.UDPConn{},
		dialingTransportsMutex: &sync.RWMutex{},
		inboundTransports:      &sync.Map{},
		outboundTransports:     &sync.Map{},
		logger:                 logs.NewLogger(streamManagerCaller),
	}
	return sm
}

func (sm *babelStreamManager) Logger() *log.Logger {
	return sm.logger
}

func (sm *babelStreamManager) SendMessageSideStream(toSend message.Message, peer peer.Peer, rAddrInt net.Addr, sourceProtoID protocol.ID, destProto protocol.ID) errors.Error {
	switch rAddr := rAddrInt.(type) {
	case *net.UDPAddr:
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, msgBytes)
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		peerBytes := sm.babel.SelfPeer().Marshal()
		_, wErr := sm.udpConn.WriteToUDP(append(peerBytes, wrappedBytes...), rAddr)
		if wErr != nil {
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
	case *net.TCPAddr:
		tcpStream, err := net.Dial(rAddr.Network(), rAddr.String())
		if err != nil {
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
		frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, tcpStream)
		hErr := sm.sendHandshakeMessage(frameBasedConn, sourceProtoID, TemporaryTunnel)
		if hErr != nil {
			return hErr
		}
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, msgBytes)
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		wErr := frameBasedConn.WriteFrame(wrappedBytes)
		if wErr != nil {
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		tcpStream.Close()
	default:
		log.Panicf("Unknown addr type %s", reflect.TypeOf(rAddr))
	}
	return nil
}

func (sm *babelStreamManager) AcceptConnectionsAndNotify(lAddrInt net.Addr) chan interface{} {
	done := make(chan interface{})
	go func() {
		sm.logger.Infof("Starting listener of type %s", lAddrInt.Network())
		switch lAddr := lAddrInt.(type) {
		case *net.TCPAddr:
			listener, err := net.ListenTCP(lAddr.Network(), lAddr)
			if err != nil {
				sm.logger.Panic(err)
			}
			close(done)
			sm.logger.Infof("Listening on addr: %s", lAddr)
			for {
				newStream, err := listener.Accept()
				if err != nil {
					sm.logger.Panic(err)
				}
				go func() {
					frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, newStream)
					handshakeMsg, err := sm.waitForHandshakeMessage(frameBasedConn)
					if err != nil {
						err.Log(sm.logger)
						frameBasedConn.Close()
						sm.logStreamManagerState()
						return
					}
					remotePeer := handshakeMsg.Peer
					//sm.logger.Infof("Got handshake message %+v from peer %s", handshakeMsg, remotePeer.ToString())
					if handshakeMsg.TunnelType == TemporaryTunnel {
						go sm.handleTmpStream(remotePeer, frameBasedConn)
						return
					}

					sm.logger.Infof("New connection from %s", remotePeer.String())
					if !sm.babel.InConnRequested(handshakeMsg.DialerProto, remotePeer) {
						frameBasedConn.Close()
						sm.inboundTransports.Delete(remotePeer.String())
						sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.String())
						sm.logStreamManagerState()
						return
					}

					err = sm.sendHandshakeMessage(frameBasedConn, 0, PermanentTunnel)
					if err != nil {
						sm.logger.Errorf("An error occurred during handshake with %s: %s", remotePeer.String(), err.Reason())
						sm.inboundTransports.Delete(remotePeer.String())
						frameBasedConn.Close()
						sm.logStreamManagerState()
						return
					}

					sm.logger.Warnf("Accepted connection from %s successfully", remotePeer.String())
					sm.logStreamManagerState()
					sm.inboundTransports.Store(remotePeer.String(), newStream)
					go sm.handleInStream(frameBasedConn, remotePeer)
				}()
			}
		case *net.UDPAddr:
			deserializer := internalMsg.AppMessageWrapperSerializer{}
			packetConn, err := net.ListenUDP(lAddr.Network(), lAddr)
			if err != nil {
				sm.logger.Panic(err)
			}
			sm.udpConn = packetConn
			close(done)
			for {
				msgBytes := make([]byte, 2048)
				n, _, err := packetConn.ReadFrom(msgBytes)
				if err != nil {
					sm.logger.Panic(err)
				}
				sender := &peer.IPeer{}
				msgBuf := msgBytes[:n]
				peerSize := sender.Unmarshal(msgBuf)
				sm.logger.Info(sender.String())

				deserialized := deserializer.Deserialize(msgBuf[peerSize:])
				protoMsg := deserialized.(*internalMsg.AppMessageWrapper)
				// sm.logger.Infof("Got message via UDP: %+v from %s", protoMsg, sender)
				appMsg := sm.babel.SerializationManager().Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
				sm.babel.DeliverMessage(sender, appMsg, protoMsg.DestProto)
			}
		default:
			sm.logger.Panic("cannot listen in such addr")
		}
	}()
	return done
}

func (sm *babelStreamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, addr net.Addr) errors.Error {
	sm.logger.Warnf("Dialing: %s", toDial.String())

	// check-lock-check
	k := getKeyForConn(dialingProto, toDial)
	_, connUp := sm.outboundTransports.Load(k)
	if connUp {
		return errors.NonFatalError(500, "connection already up", streamManagerCaller)
	}
	sm.dialingTransportsMutex.Lock()
	_, connUp = sm.outboundTransports.Load(k)
	if connUp {
		sm.dialingTransportsMutex.Unlock()
		return errors.NonFatalError(500, "connection already up", streamManagerCaller)
	}
	newOutboundTransport := &outboundTransport{
		Addr:     addr,
		DialErr:  make(chan interface{}),
		Dialed:   make(chan interface{}),
		Finished: make(chan interface{}),
		MsgChan:  make(chan []byte, 15),
	}
	sm.outboundTransports.Store(k, newOutboundTransport)
	sm.dialingTransportsMutex.Unlock()
	go func() {
		conn, err := net.DialTimeout(addr.Network(), addr.String(), sm.conf.DialTimeout)
		if err != nil {
			sm.logger.Error(err)
			close(newOutboundTransport.DialErr)
			sm.babel.DialError(dialingProto, toDial)
			sm.outboundTransports.Delete(k)
			return
		}
		// sm.logger.Infof("Done dialing node %s", toDial.Addr())
		// sm.logger.Infof("Exchanging protos")
		//sm.logger.Info("Remote protos: %d", handshakeMsg.Protos)
		//sm.logger.Infof("Starting handshake...")

		switch newStreamTyped := conn.(type) {
		case net.Conn:
			frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, newStreamTyped)
			herr := sm.sendHandshakeMessage(frameBasedConn, dialingProto, PermanentTunnel)
			if herr != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Error())
				frameBasedConn.Close()
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}
			_, err := sm.waitForHandshakeMessage(frameBasedConn)
			if err != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Reason())
				frameBasedConn.Close()
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}

			if !sm.babel.DialSuccess(dialingProto, toDial) {
				sm.logger.Error("No protocol accepted conn")
				frameBasedConn.Close()
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}
			close(newOutboundTransport.Dialed)
			sm.logger.Warnf("Dialed %s successfully", k)
			sm.logStreamManagerState()
			sm.handleOutTransportFrameConn(dialingProto, newOutboundTransport, frameBasedConn, toDial)
		default:
			sm.logger.Panic("Unsupported conn type")
		}
	}()
	return nil
}

func (sm *babelStreamManager) handleOutTransportFrameConn(dialingProto protocol.ID, t *outboundTransport, conn messageIO.FrameConn, peer peer.Peer) {
	for msg := range t.MsgChan {
		err := conn.WriteFrame(msg)
		if err != nil {
			conn.Close()
			close(t.Finished)
			sm.handleOutboundTransportFailure(dialingProto, t, peer)
		}
	}
}

func (sm *babelStreamManager) handleOutboundTransportFailure(dialingProto protocol.ID, t *outboundTransport, remotePeer peer.Peer) errors.Error {
	k := getKeyForConn(dialingProto, remotePeer)
	sm.dialingTransportsMutex.Lock()
	sm.outboundTransports.Delete(k)
	sm.dialingTransportsMutex.Unlock()
	sm.babel.OutTransportFailure(dialingProto, remotePeer)
	return nil
}

func (sm *babelStreamManager) SendMessage(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destination protocol.ID) errors.Error {
	sm.dialingTransportsMutex.RLock()
	k := getKeyForConn(origin, destPeer)
	outboundStreamInt, ok := sm.outboundTransports.Load(k)
	if !ok {
		sm.dialingTransportsMutex.RUnlock()
		return errors.NonFatalError(404, "stream not found", streamManagerCaller)
	}
	outboundStream := outboundStreamInt.(*outboundTransport)

	//p.logger.Infof("Sending message of type %s", reflect.TypeOf(toSend))
	// p.logger.Infof("Sending (bytes): %+v", toSendBytes)
	sm.dialingTransportsMutex.RUnlock()
	select {
	case <-outboundStream.DialErr:
		return errors.NonFatalError(500, "dial failed", streamManagerCaller)
	case <-outboundStream.Finished:
		return errors.NonFatalError(500, "connection error", streamManagerCaller)
	case <-outboundStream.Dialed:
		select {
		case <-outboundStream.Finished:
			return errors.NonFatalError(500, "connection error", streamManagerCaller)
		case outboundStream.MsgChan <- appMsgSerializer.Serialize(internalMsg.NewAppMessageWrapper(toSend.Type(), origin, destination, sm.babel.SerializationManager().Serialize(toSend))):
		}
	}
	return nil
}

func (sm *babelStreamManager) Disconnect(disconnectingProto protocol.ID, p peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Disconnecting from %s", p.String())
	sm.dialingTransportsMutex.Lock()
	k := getKeyForConn(disconnectingProto, p)
	if conn, ok := sm.outboundTransports.Load(k); ok {
		sm.outboundTransports.Delete(k)
		close(conn.(*outboundTransport).MsgChan)
	}
	sm.dialingTransportsMutex.Unlock()
	sm.logStreamManagerState()
}

func (sm *babelStreamManager) handleInStream(mr messageIO.FrameConn, newPeer peer.Peer) {
	sm.logger.Warnf("[ConnectionEvent] : Handling peer stream %s", newPeer.String())
	defer sm.logger.Warnf("[ConnectionEvent] : Done handling peer stream %s", newPeer.String())

	deserializer := internalMsg.AppMessageWrapperSerializer{}
	for {
		msgBuf, err := mr.ReadFrame()
		if err != nil {
			if err == io.EOF {
				sm.logger.Warnf("Read routine from %s got %s, exiting cleanly...", newPeer.String(), err)
			} else {
				sm.logger.Error(err)
			}
			mr.Close()
			mr = nil
			sm.inboundTransports.Delete(newPeer.String())
			return
		}
		if len(msgBuf) > 0 {
			go func() {
				//sm.logger.Infof("Read %d bytes from %s", n, newPeer.ToString())
				msgGeneric := deserializer.Deserialize(msgBuf)
				msgWrapper := msgGeneric.(*internalMsg.AppMessageWrapper)
				appMsg := sm.babel.SerializationManager().Deserialize(msgWrapper.MessageID, msgWrapper.WrappedMsgBytes)
				sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
			}()
		}
	}
}

func (sm *babelStreamManager) handleTmpStream(newPeer peer.Peer, c messageIO.FrameConn) {
	deserializer := internalMsg.AppMessageWrapperSerializer{}

	if newPeer.String() == sm.babel.SelfPeer().String() {
		sm.logger.Panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	msgBytes, err := c.ReadFrame()
	if err != nil {
		return
	}
	msgGeneric := deserializer.Deserialize(msgBytes)
	msgWrapper := msgGeneric.(*internalMsg.AppMessageWrapper)
	//sm.logger.Info("Done reading from tmp stream")
	appMsg := sm.babel.SerializationManager().Deserialize(msgWrapper.MessageID, msgWrapper.WrappedMsgBytes)
	sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
	c.Close()
}

func (sm *babelStreamManager) waitForHandshakeMessage(frameBasedConn messageIO.FrameConn) (*internalMsg.ProtoHandshakeMessage, errors.Error) {
	msgBytes, err := frameBasedConn.ReadFrame()
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes).(internalMsg.ProtoHandshakeMessage)
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return &msg, nil
}

func (sm *babelStreamManager) sendHandshakeMessage(transport messageIO.FrameConn, dialerProto protocol.ID, chanType uint8) errors.Error {
	var toSend = internalMsg.NewProtoHandshakeMessage(dialerProto, sm.babel.SelfPeer(), chanType)
	msgBytes := protoMsgSerializer.Serialize(toSend)
	err := transport.WriteFrame(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}

func getKeyForConn(protoId protocol.ID, peer peer.Peer) string {
	return peer.String()
}

func (sm *babelStreamManager) logStreamManagerState() {
	inboundNr := 0
	outboundNr := 0
	toLog := "inbound connections : "
	sm.inboundTransports.Range(func(peer, conn interface{}) bool {
		toLog += fmt.Sprintf("%s, ", peer.(string))
		inboundNr++
		return true
	})
	sm.logger.Info(toLog)
	toLog = ""
	toLog = "outbound connections : "
	sm.outboundTransports.Range(func(peer, conn interface{}) bool {
		toLog += fmt.Sprintf("%s, ", peer.(string))
		outboundNr++
		return true
	})
	sm.logger.Info(toLog)
}
