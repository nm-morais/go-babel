package pkg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	internalMsg "github.com/nm-morais/go-babel/internal/message"
	timerQueue "github.com/nm-morais/go-babel/pkg/dataStructures/timedEventQueue"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/streamManager"
	log "github.com/sirupsen/logrus"
	"github.com/smallnest/goframe"
)

const (
	MaxUDPMsgSize = 65535
)

var (
	ErrConnectionClosed        = fmt.Errorf("connection closed")
	streamManagerEncoderConfig = goframe.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}

	streamManagerDecoderConfig = goframe.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
)

type StreamManagerConf struct {
	BatchMaxSizeBytes int
	BatchTimeout      time.Duration
	DialTimeout       time.Duration
}

type babelStreamManager struct {
	conf               StreamManagerConf
	sentCounterMux     *sync.Mutex
	receivedCounterMux *sync.Mutex
	msgCountersSent    map[protocol.ID]int64
	msgCountersRecvd   map[protocol.ID]int64
	teq                timerQueue.TimedEventQueue
	babel              protocolManager.ProtocolManager
	udpConn            *net.UDPConn
	outboundTransports *sync.Map
	inboundTransports  *sync.Map
	logger             *log.Logger
}

type batch struct {
	mu   *sync.Mutex
	size int
	msgs []*msgBytesWithCallback
}

type msgBytesWithCallback struct {
	msgBytes []byte
	Callback func(error)
}

type outboundTransport struct {
	k    int
	Addr net.Addr

	Dialed      chan interface{}
	DialErr     chan interface{}
	Finished    chan interface{}
	doneWriting chan interface{}
	writers     *sync.WaitGroup
	ToWrite     chan []*msgBytesWithCallback

	conn goframe.FrameConn

	targetPeer  peer.Peer
	originProto protocol.ID
	batch       *batch
	babel       protocolManager.ProtocolManager
	sm          *babelStreamManager
	closeOnce   *sync.Once
}

func (ot *outboundTransport) ID() string {
	return fmt.Sprintf("%d", ot.k)
}

func (ot *outboundTransport) OnTrigger() (reAdd bool, nextDeadline *time.Time) {
	select {
	case <-ot.Finished:
		return false, nil
	default:
		ot.batch.mu.Lock()
		defer ot.batch.mu.Unlock()
		ot.flushBatch()
		nextDeadline := time.Now().Add(ot.sm.conf.BatchTimeout)
		return true, &nextDeadline
	}
}

func (ot *outboundTransport) flushBatch() {
	defer func() {
		if x := recover(); x != nil {
			ot.sm.logger.Panicf("Panic in flush batch: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()
	// ot.sm.logger.Infof("Flushing batch...")
	if ot.batch.msgs != nil {
		ot.sendMessagesToChan(ot.batch.msgs...)
		ot.batch.msgs = nil
		ot.batch.size = 0
	}
}

func (ot *outboundTransport) sendMessagesToChan(messages ...*msgBytesWithCallback) {
	ot.writers.Add(1)
	defer ot.writers.Done()
	select {
	case <-ot.Finished:
		for _, msg := range messages {
			msg.Callback(ErrConnectionClosed)
		}
	case ot.ToWrite <- messages:
	}
}

func (ot *outboundTransport) writeOutboundMessages() {
	sendToConn := func(msgs []*msgBytesWithCallback) error {
		var toSend []byte = nil
		for _, p := range msgs {
			toSend = append(toSend, p.msgBytes...)
		}
		err := ot.conn.WriteFrame(toSend)
		for _, cb := range msgs {
			go cb.Callback(err)
		}
		if err != nil {
			ot.sm.logger.Errorf("Out connection got unexpected error: %s", err.Error())
		}
		return err
	}

	defer func() {
	outer:
		for {
			select {
			case msgs := <-ot.ToWrite:
				sendToConn(msgs)
			default:
				break outer
			}
		}
		close(ot.doneWriting)
		ot.close(true)
	}()

	for {
		select {
		case packet, ok := <-ot.ToWrite:
			if !ok {
				ot.sm.logger.Panic("Shouldn't happen")
			}
			err := sendToConn(packet)
			if err != nil {
				return
			}
		case <-ot.Finished:
			return
		}
	}
}

func (ot *outboundTransport) sendMessage(originProto, destProto protocol.ID, toSend message.Message, dest peer.Peer, batch bool) {
	defer func() {
		if x := recover(); x != nil {
			ot.sm.logger.Panicf("Panic in sendMessage: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()
	select {
	case <-ot.DialErr:
		ot.sm.babel.MessageDeliveryErr(originProto, toSend, dest, errors.NonFatalError(500, "dial failed", streamManagerCaller))
	case <-ot.Finished:
		ot.sm.babel.MessageDeliveryErr(originProto, toSend, dest, errors.NonFatalError(500, "stream finished", streamManagerCaller))
	case <-ot.Dialed:
		internalMsgBytes, _ := ot.sm.babel.SerializationManager().Serialize(toSend)
		msgBytes := internalMsg.NewAppMessageWrapper(
			toSend.Type(),
			originProto,
			destProto,
			ot.sm.babel.SelfPeer(),
			internalMsgBytes,
		).Serialize()
		if batch {
			// ot.sm.logger.Infof("Added message of type %s to batch to %s", reflect.TypeOf(toSend), dest.String())
			ot.batch.mu.Lock()
			defer ot.batch.mu.Unlock()
			if ot.batch.size+len(msgBytes) >= ot.sm.conf.BatchMaxSizeBytes {
				ot.flushBatch()
			}
			ot.batch.msgs = append(ot.batch.msgs, &msgBytesWithCallback{
				msgBytes: msgBytes,
				Callback: func(err error) {
					ot.messageCallback(err, toSend, originProto)
				},
			})
			ot.batch.size += len(msgBytes)
		} else {
			ot.sendMessagesToChan(&msgBytesWithCallback{
				msgBytes: msgBytes,
				Callback: func(err error) {
					ot.messageCallback(err, toSend, originProto)
				},
			})
		}
	}
}

func (ot *outboundTransport) messageCallback(err error, msg message.Message, originProto protocol.ID) {
	if err != nil {
		ot.sm.babel.MessageDeliveryErr(originProto, msg, ot.targetPeer, errors.NonFatalError(500, err.Error(), streamManagerCaller))
	} else {
		ot.sm.babel.MessageDelivered(originProto, msg, ot.targetPeer)
		ot.sm.addMsgSent(originProto)
	}
}

func (ot *outboundTransport) close(notifyProto bool) {
	ot.closeOnce.Do(func() {
		ot.batch.mu.Lock()
		defer ot.batch.mu.Unlock()
		for _, msgGeneric := range ot.batch.msgs {
			msgGeneric.Callback(fmt.Errorf("disconnected from peer"))
		}
		ot.batch.msgs = nil
		ot.batch.size = 0
		select {
		case <-ot.Finished:
		default:
			close(ot.Finished)
			ot.writers.Wait()
			<-ot.doneWriting
			close(ot.ToWrite)
			ot.sm.closeConn(ot.conn)
			if notifyProto {
				ot.sm.babel.OutTransportFailure(ot.originProto, ot.targetPeer)
			}
			ot.sm.teq.Remove(ot.ID())
			ot.sm.outboundTransports.Delete(ot.targetPeer.String())
		}
	})
}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

const streamManagerCaller = "StreamManager"

func NewStreamManager(babel protocolManager.ProtocolManager, conf StreamManagerConf) streamManager.StreamManager {
	logger := logs.NewLogger(streamManagerCaller)

	sm := &babelStreamManager{
		conf:               conf,
		sentCounterMux:     &sync.Mutex{},
		receivedCounterMux: &sync.Mutex{},
		msgCountersSent:    map[uint16]int64{},
		msgCountersRecvd:   map[uint16]int64{},
		teq:                timerQueue.NewTimedEventQueue(logger),
		babel:              babel,
		udpConn:            &net.UDPConn{},
		outboundTransports: &sync.Map{},
		inboundTransports:  &sync.Map{},
		logger:             logger,
	}
	sm.logger.Infof("Starting streamManager with config: %+v", conf)

	go func() {
		for range time.NewTicker(10 * time.Second).C {
			sm.logMsgStats()
		}
	}()
	return sm
}

func (sm *babelStreamManager) Logger() *log.Logger {
	return sm.logger
}

func (sm *babelStreamManager) SendMessageSideStream(
	toSend message.Message,
	peer peer.Peer,
	rAddrInt net.Addr,
	sourceProtoID protocol.ID,
	destProto protocol.ID,
) errors.Error {
	switch rAddr := rAddrInt.(type) {
	case *net.UDPAddr:
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, sm.babel.SelfPeer(), msgBytes)
		wrappedBytes := msgWrapper.Serialize()
		peerBytes := sm.babel.SelfPeer().Marshal()
		bytesToSend := append(peerBytes, wrappedBytes...)
		if len(bytesToSend) > MaxUDPMsgSize {
			sm.logger.Panicf("Size message exceeded: (%d/%d)", len(bytesToSend), MaxUDPMsgSize)
		}
		written, wErr := sm.udpConn.WriteToUDP(bytesToSend, rAddr)
		if wErr != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, errors.NonFatalError(500, wErr.Error(), streamManagerCaller))
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		if written != len(bytesToSend) {
			sm.logger.Panicf("Did not send all message bytes: (%d/%d)", written, len(bytesToSend))
		}

		sm.babel.MessageDelivered(sourceProtoID, toSend, peer)
		sm.addMsgSent(sourceProtoID)
	case *net.TCPAddr:
		tcpStream, err := net.DialTimeout(rAddr.Network(), rAddr.String(), sm.conf.DialTimeout)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				sm.logger.Errorf("Got timeout error dialing with dialTimeout=%+v", sm.conf.DialTimeout)
			} else {
				sm.logger.Errorf("Got error dialing: %s", err.Error())
			}
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, errors.NonFatalError(500, err.Error(), streamManagerCaller))
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
		defer tcpStream.Close()
		// conn := gev.
		frameBasedConn := goframe.NewLengthFieldBasedFrameConn(streamManagerEncoderConfig, streamManagerDecoderConfig, tcpStream)
		hErr := sm.sendHandshakeMessage(frameBasedConn, sourceProtoID, TemporaryTunnel)
		if hErr != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, hErr)
			return hErr
		}
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, sm.babel.SelfPeer(), msgBytes)
		wrappedBytes := msgWrapper.Serialize()
		wErr := frameBasedConn.WriteFrame(wrappedBytes)
		if wErr != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, errors.NonFatalError(500, wErr.Error(), streamManagerCaller))
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		sm.babel.MessageDelivered(sourceProtoID, toSend, peer)
		sm.addMsgSent(sourceProtoID)
	default:
		log.Panicf("Unknown addr type %s", reflect.TypeOf(rAddr))
	}
	return nil
}

func (sm *babelStreamManager) addMsgSent(proto protocol.ID) {
	sm.sentCounterMux.Lock()
	sm.msgCountersSent[proto]++
	sm.sentCounterMux.Unlock()
}

func (sm *babelStreamManager) addMsgReceived(proto protocol.ID) {
	sm.receivedCounterMux.Lock()
	sm.msgCountersRecvd[proto]++
	sm.receivedCounterMux.Unlock()
}

func (sm *babelStreamManager) closeConn(c goframe.FrameConn) {
	err := c.Close()
	if err != nil {
		sm.logger.Errorf("Err: %+w", err)
	}
}

func (sm *babelStreamManager) AcceptConnectionsAndNotify(lAddrInt net.Addr) chan interface{} {
	done := make(chan interface{})
	go func() {
		sm.logger.Infof("Starting listener of type %s on addr: %s", lAddrInt.Network(), lAddrInt.String())
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
					defer func() {
						if x := recover(); x != nil {
							sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))
						}
					}()
					frameBasedConn := goframe.NewLengthFieldBasedFrameConn(streamManagerEncoderConfig, streamManagerDecoderConfig, newStream)

					defer sm.closeConn(frameBasedConn)
					handshakeMsg, err := sm.waitForHandshakeMessage(frameBasedConn)
					if err != nil {
						err.Log(sm.logger)
						sm.logStreamManagerState()
						return
					}
					remotePeer := handshakeMsg.Peer
					if handshakeMsg.TunnelType == TemporaryTunnel {
						sm.handleTmpStream(remotePeer, frameBasedConn)
						return
					}

					sm.logger.Infof("New connection from %s", remotePeer.String())
					if !sm.babel.InConnRequested(handshakeMsg.DialerProto, remotePeer) {
						defer sm.closeConn(frameBasedConn)
						_, _ = io.Copy(ioutil.Discard, newStream)
						sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.String())
						sm.logStreamManagerState()
						return
					}

					sm.logger.Infof("Accepted connection from %s successfully", remotePeer.String())
					sm.logStreamManagerState()
					sm.inboundTransports.Store(remotePeer.String(), newStream)
					sm.handleInStream(frameBasedConn, remotePeer)
					sm.inboundTransports.Delete(remotePeer.String())
				}()
			}
		case *net.UDPAddr:
			packetConn, err := net.ListenUDP(lAddr.Network(), lAddr)
			if err != nil {
				sm.logger.Panic(err)
			}
			sm.udpConn = packetConn
			close(done)
			for {
				msgBytes := make([]byte, MaxUDPMsgSize)
				n, _, err := packetConn.ReadFrom(msgBytes)
				if err != nil {
					sm.logger.Panic(err)
				}
				sender := &peer.IPeer{}
				peerSize := sender.Unmarshal(msgBytes)
				msg := internalMsg.AppMessageWrapper{}
				_, err = msg.Deserialize(msgBytes[peerSize:n])
				if err != nil {
					sm.logger.Errorf("Error %s deserializing message of type AppMessageWrapper from node %s", err.Error(), sender.String())
					continue
				}
				appMsg, err := sm.babel.SerializationManager().Deserialize(msg.MessageID, msg.WrappedMsgBytes)
				if err != nil {
					sm.logger.Errorf("Error %s deserializing message of type %d from node %s", err.Error(), msg.MessageID, sender)
					continue
				}
				sm.babel.DeliverMessage(sender, appMsg, msg.DestProto)
				sm.addMsgReceived(msg.DestProto)
			}
		default:
			sm.logger.Panic("cannot listen in such addr")
		}
	}()
	return done
}

func (sm *babelStreamManager) DialAndNotify(dialingProto protocol.ID, toDial peer.Peer, addr net.Addr) errors.Error {
	k := GetKeyForConn(dialingProto, toDial)
	newOutboundTransport := &outboundTransport{
		k:           rand.Int(),
		Addr:        addr,
		Dialed:      make(chan interface{}),
		DialErr:     make(chan interface{}),
		Finished:    make(chan interface{}),
		doneWriting: make(chan interface{}),
		writers:     &sync.WaitGroup{},
		ToWrite:     make(chan []*msgBytesWithCallback, 1024),
		conn:        nil,
		targetPeer:  toDial,
		originProto: dialingProto,
		batch:       &batch{mu: &sync.Mutex{}, size: sm.conf.BatchMaxSizeBytes, msgs: nil},
		babel:       sm.babel,
		sm:          sm,
		closeOnce:   &sync.Once{},
	}
	newOutboundTransport.writers.Add(1)
	_, loaded := sm.outboundTransports.LoadOrStore(k, newOutboundTransport)
	if loaded {
		sm.logger.Warnf("Stream to %s already existed", toDial.String())
		return errors.NonFatalError(500, "connection already up", streamManagerCaller)
	}
	go func() {
		defer newOutboundTransport.writers.Done()
		defer func() {
			if x := recover(); x != nil {
				sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))

			}
		}()
		sm.logger.Infof("Dialing peer: %s", toDial.String())
		conn, err := net.DialTimeout(addr.Network(), addr.String(), sm.conf.DialTimeout)
		if err != nil {
			sm.logger.Error(err)
			close(newOutboundTransport.DialErr)
			sm.babel.DialError(dialingProto, toDial)
			sm.outboundTransports.Delete(k)
			return
		}
		switch newStreamTyped := conn.(type) {
		case net.Conn:
			frameBasedConn := goframe.NewLengthFieldBasedFrameConn(streamManagerEncoderConfig, streamManagerDecoderConfig, newStreamTyped)
			newOutboundTransport.conn = frameBasedConn

			herr := sm.sendHandshakeMessage(frameBasedConn, dialingProto, PermanentTunnel)
			if herr != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), herr)
				sm.closeConn(frameBasedConn)
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}

			if !sm.babel.DialSuccess(dialingProto, toDial) {
				sm.logger.Error("protocol did not accept conn")
				sm.closeConn(frameBasedConn)
				close(newOutboundTransport.DialErr)
				sm.outboundTransports.Delete(k)
				return
			}

			close(newOutboundTransport.Dialed)
			sm.logger.Infof("Dialed %s successfully", k)
			sm.teq.Add(newOutboundTransport, time.Now().Add(sm.conf.BatchTimeout))
			sm.logStreamManagerState()

			go newOutboundTransport.writeOutboundMessages()

		default:
			sm.logger.Panic("Unsupported conn type")
		}
	}()
	return nil
}

func (sm *babelStreamManager) SendMessage(
	toSend message.Message,
	destPeer peer.Peer,
	origin protocol.ID,
	destination protocol.ID,
	batch bool,
) {
	defer func() {
		if x := recover(); x != nil {
			sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()
	k := GetKeyForConn(origin, destPeer)
	outboundStreamInt, ok := sm.outboundTransports.Load(k)
	if !ok {
		sm.babel.MessageDeliveryErr(origin, toSend, destPeer, errors.NonFatalError(404, "stream not found", streamManagerCaller))
		return
	}
	outboundStreamInt.(*outboundTransport).sendMessage(origin, destination, toSend, destPeer, batch)
}

func (sm *babelStreamManager) Disconnect(disconnectingProto protocol.ID, p peer.Peer) {
	defer func() {
		if x := recover(); x != nil {
			sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()

	k := GetKeyForConn(disconnectingProto, p)
	outboundStreamInt, loaded := sm.outboundTransports.LoadAndDelete(k)
	if loaded {
		outboundStreamInt.(*outboundTransport).close(false)
	}
}

func (sm *babelStreamManager) handleInStream(mr goframe.FrameConn, newPeer peer.Peer) {
	logger := sm.logger.WithField("stream", newPeer.String())

	logger.Info("[ConnectionEvent] : Started handling peer stream")
	defer logger.Info("[ConnectionEvent] : Done handling peer stream")
	carry := []byte{}
	for {
		newFrame, err := mr.ReadFrame()
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Read routine got unusual error: %s", err.Error())
			}
			return
		}

		if len(carry) > 0 {
			newFrame = append(carry, newFrame...)
			carry = nil
		}

		if len(newFrame) == 0 {
			logger.Error("got 0 bytes from conn")
			continue
		}
		curr := 0
		for curr < len(newFrame) {
			msgWrapper := &internalMsg.AppMessageWrapper{}
			_, err := msgWrapper.Deserialize(newFrame[curr:])
			if err != nil {
				if err.Error() == internalMsg.ErrNotEnoughLen.Error() {
					carry = newFrame
					continue
				}
			}
			curr += msgWrapper.TotalMessageBytes
			appMsg, err := sm.babel.SerializationManager().Deserialize(msgWrapper.MessageID, msgWrapper.WrappedMsgBytes)
			if err != nil {
				logger.Errorf("Got error %s deserializing message of type %d from %s", err.Error(), msgWrapper.MessageID, newPeer)
				continue
			}
			sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
			sm.addMsgReceived(msgWrapper.DestProto)
		}
	}
}

func (sm *babelStreamManager) handleTmpStream(newPeer peer.Peer, c goframe.FrameConn) {
	if peer.PeersEqual(newPeer, sm.babel.SelfPeer()) {
		sm.logger.Panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	msgBytes, connErr := c.ReadFrame()
	msgWrapper := &internalMsg.AppMessageWrapper{}
	if len(msgBytes) > 0 {
		_, err := msgWrapper.Deserialize(msgBytes)
		if err != nil {
			sm.logger.Errorf("Error deserializing message in tmp stream: %s", err.Error())
			return
		}
	}
	if connErr != nil {
		sm.logger.Errorf("Error reading in tmp stream: %s", connErr.Error())
		return
	}
	//sm.logger.Info("Done reading from tmp stream")
	appMsg, err := sm.babel.SerializationManager().Deserialize(msgWrapper.MessageID, msgWrapper.WrappedMsgBytes)
	if err != nil {
		sm.logger.Panicf("Got error %s deserializing message of type %d from %s", err.Error(), msgWrapper.MessageID, newPeer)
	}
	sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
	sm.addMsgReceived(msgWrapper.DestProto)
}

func (sm *babelStreamManager) waitForHandshakeMessage(frameBasedConn goframe.FrameConn) (
	*internalMsg.ProtoHandshakeMessage,
	errors.Error,
) {
	frameBasedConn.Conn().SetReadDeadline(time.Now().Add(sm.conf.DialTimeout))
	defer frameBasedConn.Conn().SetReadDeadline(time.Time{})
	msgBytes, err := frameBasedConn.ReadFrame()
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	handshakeMsg := &internalMsg.ProtoHandshakeMessage{}
	err = handshakeMsg.Deserialize(msgBytes)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return handshakeMsg, nil
}

func (sm *babelStreamManager) sendHandshakeMessage(
	transport goframe.FrameConn,
	dialerProto protocol.ID,
	chanType uint8,
) errors.Error {
	var toSend = internalMsg.NewProtoHandshakeMessage(dialerProto, sm.babel.SelfPeer(), chanType)
	msgBytes := toSend.Serialize()
	err := transport.WriteFrame(msgBytes)
	if err != nil {
		return errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	return nil
}

func GetKeyForConn(protoId protocol.ID, peer peer.Peer) string {
	return peer.String()
}

func (sm *babelStreamManager) logStreamManagerState() {
	inboundNr := 0
	outboundNr := 0
	toLog := "inbound connections : "
	sm.inboundTransports.Range(
		func(peer, conn interface{}) bool {
			toLog += fmt.Sprintf("%s, ", peer.(string))
			inboundNr++
			return true
		},
	)
	sm.logger.Info(toLog)
	toLog = ""
	toLog = "outbound connections : "
	sm.outboundTransports.Range(
		func(peer, conn interface{}) bool {
			toLog += fmt.Sprintf("%s, ", peer.(string))
			outboundNr++
			return true
		},
	)
	sm.logger.Info(toLog)
}

func (sm *babelStreamManager) logMsgStats() {
	sm.receivedCounterMux.Lock()
	defer sm.receivedCounterMux.Unlock()
	sm.sentCounterMux.Lock()
	defer sm.sentCounterMux.Unlock()

	var toPrint = struct {
		ApplicationalMessagesSent     map[uint16]int64
		ApplicationalMessagesReceived map[uint16]int64
	}{
		ApplicationalMessagesSent:     sm.msgCountersSent,
		ApplicationalMessagesReceived: sm.msgCountersRecvd,
	}

	toPrintJSON, err := json.Marshal(toPrint)
	if err != nil {
		panic(err)
	}
	sm.logger.Infof("<app-messages-stats> %s", string(toPrintJSON))
}
