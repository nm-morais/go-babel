package pkg

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	internalMsg "github.com/nm-morais/go-babel/internal/message"
	"github.com/nm-morais/go-babel/internal/messageIO"
	priorityqueue "github.com/nm-morais/go-babel/pkg/dataStructures/priorityQueue"
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
	BatchMaxSizeBytes int
	BatchTimeout      time.Duration
	DialTimeout       time.Duration
}

type outboundTransportBatchControl = struct {
	connKey  string
	deadline time.Time
}

type babelStreamManager struct {
	conf StreamManagerConf

	babel              protocolManager.ProtocolManager
	udpConn            *net.UDPConn
	outboundTransports *sync.Map
	inboundTransports  *sync.Map
	logger             *log.Logger

	addBatchControlChan chan *outboundTransportBatchControl
}

type outboundTransport struct {
	Addr net.Addr

	Dialed   chan interface{}
	DialErr  chan interface{}
	Finished chan interface{}

	conn   messageIO.FrameConn
	connMU sync.Mutex

	targetPeer  peer.Peer
	originProto protocol.ID

	batchFlush    chan time.Time
	batchMessages []struct {
		originProto protocol.ID
		msg         message.Message
	}
	batchBytes []byte
	batchMU    sync.Mutex
}

const (
	TemporaryTunnel = 0
	PermanentTunnel = 1
)

const streamManagerCaller = "StreamManager"

func NewStreamManager(babel protocolManager.ProtocolManager, conf StreamManagerConf) *babelStreamManager {
	sm := &babelStreamManager{
		conf:                conf,
		babel:               babel,
		udpConn:             &net.UDPConn{},
		outboundTransports:  &sync.Map{},
		inboundTransports:   &sync.Map{},
		logger:              logs.NewLogger(streamManagerCaller),
		addBatchControlChan: make(chan *outboundTransportBatchControl),
	}
	sm.logger.Infof("Starting streamManager with config: %+v", conf)
	go sm.flushBatchesPeriodic()
	return sm
}

func (sm *babelStreamManager) flushBatchesPeriodic() {
	var t *time.Timer
	pq := priorityqueue.PriorityQueue{}

	addBatchControlToQueue := func(batchControl *outboundTransportBatchControl, nextTrigger time.Time) {
		// sm.logger.Infof("Adding batch control %s to queue", batchControl.connKey)
		batchControl.deadline = nextTrigger
		pqItem := &priorityqueue.Item{
			Value:    batchControl,
			Priority: nextTrigger.UnixNano(),
		}
		heap.Push(&pq, pqItem)
		heap.Init(&pq)
	}

outer:
	for {
		if len(pq) == 0 {
			// sm.logger.Info("No out connections, waiting for new connection")
			newBatchControl := <-sm.addBatchControlChan
			sm.logger.Infof("Adding new batch control to : %s", newBatchControl.connKey)
			addBatchControlToQueue(newBatchControl, time.Now().Add(sm.conf.BatchTimeout))
		}

		nextItem := heap.Pop(&pq).(*priorityqueue.Item).Value.(*outboundTransportBatchControl)
		transportInt, stillActive := sm.outboundTransports.Load(nextItem.connKey)
		if !stillActive {
			sm.logger.Warnf("batch control to %s exiting...", nextItem.connKey)
			continue
		}
		transport := transportInt.(*outboundTransport)
		select {
		case flushTime := <-transport.batchFlush:
			if flushTime.Add(sm.conf.BatchTimeout).After(nextItem.deadline) {
				// sm.logger.Infof("batch to %s was flushed while wating for other batch, adjusting next trigger", nextItem.connKey)
				addBatchControlToQueue(nextItem, flushTime.Add(sm.conf.BatchTimeout))
				continue outer
			}
		default:
		}

		// sm.logger.Infof("Next batch to dispatch: %s", nextItem.connKey)
		t = time.NewTimer(time.Until(nextItem.deadline))
		// sm.logger.Infof("Waiting %+v for next deadline...", time.Until(nextItem.deadline))
		select {
		case newBatchControl := <-sm.addBatchControlChan:
			sm.logger.Infof("Adding new batch control to : %s", newBatchControl.connKey)
			addBatchControlToQueue(newBatchControl, time.Now().Add(sm.conf.BatchTimeout))
			addBatchControlToQueue(nextItem, nextItem.deadline)
		case <-t.C:
			// sm.logger.Infof("Batch emission to %s triggered", nextItem.connKey)
			_, stillActive := sm.outboundTransports.Load(nextItem.connKey)
			if !stillActive {
				break
			}
			addBatchControlToQueue(nextItem, time.Now().Add(sm.conf.BatchTimeout))
			go func() {
				transport.batchMU.Lock()
				if len(transport.batchBytes) > 0 {
					sm.FlushBatch(nextItem.connKey, transport)
				}
				transport.batchMU.Unlock()
			}()
			// sm.logger.Infof("Re-added batch control to : %s", nextItem.connKey)
		case flushTime := <-transport.batchFlush:
			_, stillActive := sm.outboundTransports.Load(nextItem.connKey)
			if !stillActive {
				sm.logger.Warnf("batch control to %s exiting...", nextItem.connKey)
				break
			}
			addBatchControlToQueue(nextItem, flushTime.Add(sm.conf.BatchTimeout))
			// sm.logger.Infof("Re-added batch control to : %s", nextItem.connKey)
		case <-transport.Finished:
		}
		t.Stop()
	}
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
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, msgBytes)
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		peerBytes := sm.babel.SelfPeer().Marshal()
		_, wErr := sm.udpConn.WriteToUDP(append(peerBytes, wrappedBytes...), rAddr)
		if wErr != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, errors.NonFatalError(500, wErr.Error(), streamManagerCaller))
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		sm.babel.MessageDelivered(sourceProtoID, toSend, peer)
	case *net.TCPAddr:
		tcpStream, err := net.Dial(rAddr.Network(), rAddr.String())
		if err != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, errors.NonFatalError(500, err.Error(), streamManagerCaller))
			return errors.NonFatalError(500, err.Error(), streamManagerCaller)
		}
		frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, tcpStream)
		hErr := sm.sendHandshakeMessage(frameBasedConn, sourceProtoID, TemporaryTunnel)
		if hErr != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, hErr)
			return hErr
		}
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, msgBytes)
		wrappedBytes := appMsgSerializer.Serialize(msgWrapper)
		wErr := frameBasedConn.WriteFrame(wrappedBytes)
		if wErr != nil {
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, errors.NonFatalError(500, wErr.Error(), streamManagerCaller))
			return errors.NonFatalError(500, wErr.Error(), streamManagerCaller)
		}
		sm.babel.MessageDelivered(sourceProtoID, toSend, peer)
		err = tcpStream.Close()
		if err != nil {
			sm.logger.Errorf("Err: %+w", err)
		}
	default:
		log.Panicf("Unknown addr type %s", reflect.TypeOf(rAddr))
	}
	return nil
}

func (sm *babelStreamManager) closeConn(c messageIO.FrameConn) {
	err := c.Close()
	if err != nil {
		sm.logger.Errorf("Err: %+w", err)
	}
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
						defer sm.closeConn(frameBasedConn)
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
						defer sm.closeConn(frameBasedConn)
						sm.inboundTransports.Delete(remotePeer.String())
						sm.logger.Infof("Peer %s conn was not accepted, closing stream", remotePeer.String())
						sm.logStreamManagerState()
						return
					}

					err = sm.sendHandshakeMessage(frameBasedConn, 0, PermanentTunnel)
					if err != nil {
						sm.logger.Errorf(
							"An error occurred during handshake with %s: %s",
							remotePeer.String(),
							err.Reason(),
						)
						sm.inboundTransports.Delete(remotePeer.String())
						defer sm.closeConn(frameBasedConn)
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
	k := getKeyForConn(dialingProto, toDial)
	newOutboundTransportGeneric, loaded := sm.outboundTransports.LoadOrStore(k, &outboundTransport{
		Addr:        addr,
		Dialed:      make(chan interface{}),
		originProto: dialingProto,
		Finished:    make(chan interface{}),
		DialErr:     make(chan interface{}),
		conn:        nil,
		connMU:      sync.Mutex{},
		batchMU:     sync.Mutex{},
		batchMessages: make([]struct {
			originProto uint16
			msg         message.Message
		}, 0),
		batchBytes: make([]byte, 0, sm.conf.BatchMaxSizeBytes),
		targetPeer: toDial,
		batchFlush: make(chan time.Time, 1),
	})
	newOutboundTransport := newOutboundTransportGeneric.(*outboundTransport)
	if loaded {
		return errors.NonFatalError(500, "connection already up", streamManagerCaller)
	}
	go func() {
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
			frameBasedConn := messageIO.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, newStreamTyped)
			herr := sm.sendHandshakeMessage(frameBasedConn, dialingProto, PermanentTunnel)
			if herr != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), herr)
				defer sm.closeConn(frameBasedConn)
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}
			_, err := sm.waitForHandshakeMessage(frameBasedConn)
			if err != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), err.Reason())
				defer sm.closeConn(frameBasedConn)
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}

			if !sm.babel.DialSuccess(dialingProto, toDial) {
				sm.logger.Error("protocol did not accept conn")
				defer sm.closeConn(frameBasedConn)
				close(newOutboundTransport.DialErr)
				sm.outboundTransports.Delete(k)
				return
			}
			newOutboundTransport.conn = frameBasedConn
			close(newOutboundTransport.Dialed)
			sm.logger.Warnf("Dialed %s successfully", k)
			sm.addBatchControlChan <- &struct {
				connKey  string
				deadline time.Time
			}{
				connKey:  k,
				deadline: time.Now().Add(sm.conf.BatchTimeout),
			}
			sm.logStreamManagerState()
		default:
			sm.logger.Panic("Unsupported conn type")
		}

	}()
	return nil
}

// func (sm *babelStreamManager) handleOutTransportFrameConn(
// 	dialingProto protocol.ID,
// 	t *outboundTransport,
// 	peer peer.Peer,
// ) {
// 	k := getKeyForConn(dialingProto, peer)
// 	defer close(t.Finished)
// 	defer sm.outboundTransports.Delete(k)
// 	for msg := range t.MsgChan {
// 		err := t.conn.WriteFrame(msg)
// 		if err != nil {
// 			sm.closeConn(conn)
// 			sm.babel.OutTransportFailure(dialingProto, peer)
// 			break
// 		}
// 	}
// }

func (sm *babelStreamManager) SendMessage(
	toSend message.Message,
	destPeer peer.Peer,
	origin protocol.ID,
	destination protocol.ID,
	batch bool,
) errors.Error {
	k := getKeyForConn(origin, destPeer)
	outboundStreamInt, ok := sm.outboundTransports.Load(k)
	if !ok {
		sm.babel.MessageDeliveryErr(origin, toSend, destPeer, errors.NonFatalError(404, "stream not found", streamManagerCaller))
		return nil
	}

	outboundStream := outboundStreamInt.(*outboundTransport)
	select {
	case <-outboundStream.DialErr:
		sm.babel.MessageDeliveryErr(origin, toSend, destPeer, errors.NonFatalError(500, "dial failed", streamManagerCaller))
		return errors.NonFatalError(500, "dial failed", streamManagerCaller)
	case <-outboundStream.Finished:
		err := errors.NonFatalError(500, "conn finished", streamManagerCaller)
		sm.babel.MessageDeliveryErr(origin, toSend, destPeer, err)
		return err
	case <-outboundStream.Dialed:
		msgBytes := appMsgSerializer.Serialize(
			internalMsg.NewAppMessageWrapper(
				toSend.Type(),
				origin,
				destination,
				sm.babel.SerializationManager().Serialize(toSend),
			))
		sizeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBytes, uint32(len(msgBytes)))
		msgBytes = append(sizeBytes, msgBytes...)
		if batch {
			outboundStream.batchMU.Lock()
			defer outboundStream.batchMU.Unlock()
			outboundStream.batchMessages = append(outboundStream.batchMessages, struct {
				originProto uint16
				msg         message.Message
			}{
				originProto: origin,
				msg:         toSend,
			})
			// sm.logger.Infof("Added message of type %s to batch to %s", reflect.TypeOf(toSend), destPeer.String())

			outboundStream.batchBytes = append(outboundStream.batchBytes, msgBytes...)
			if len(outboundStream.batchBytes) > sm.conf.BatchMaxSizeBytes {
				return sm.FlushBatch(k, outboundStream)
			}
			return nil
		}

		outboundStream.connMU.Lock()
		err := outboundStream.conn.WriteFrame(msgBytes)
		if err != nil {
			select {
			case <-outboundStream.Finished:
			default:
				close(outboundStream.Finished)
			}
			outboundStream.connMU.Unlock()
			sm.babel.MessageDeliveryErr(origin, toSend, destPeer, errors.NonFatalError(500, err.Error(), streamManagerCaller))
			outboundStreamInt, loaded := sm.outboundTransports.LoadAndDelete(k)
			if loaded {
				outboundStream = outboundStreamInt.(*outboundTransport)
				sm.closeConn(outboundStream.conn)
				sm.babel.OutTransportFailure(origin, destPeer)
			}
			return errors.NonFatalError(500, "error sending message", streamManagerCaller)
		}
		outboundStream.connMU.Unlock()
		sm.babel.MessageDelivered(origin, toSend, destPeer)
		return nil
	}
}

func (sm *babelStreamManager) FlushBatch(streamKey string, outboundStream *outboundTransport) errors.Error {
	outboundStream.connMU.Lock()
	err := outboundStream.conn.WriteFrame(outboundStream.batchBytes)
	if err != nil {
		select {
		case <-outboundStream.Finished:
		default:
			close(outboundStream.Finished)
		}
		outboundStream.connMU.Unlock()
		for _, msgGeneric := range outboundStream.batchMessages {
			sm.babel.MessageDeliveryErr(msgGeneric.originProto, msgGeneric.msg, outboundStream.targetPeer, errors.NonFatalError(500, err.Error(), streamManagerCaller))
		}
		outboundStreamInt, loaded := sm.outboundTransports.LoadAndDelete(streamKey)
		if loaded {
			outboundStream := outboundStreamInt.(*outboundTransport)
			sm.closeConn(outboundStream.conn)
			sm.babel.OutTransportFailure(outboundStream.originProto, outboundStream.targetPeer)
		}
		return errors.NonFatalError(500, "error sending message", streamManagerCaller)
	}
	outboundStream.connMU.Unlock()
	// sm.logger.Infof("delivered batch to %s successfully", outboundStream.targetPeer.String())
	for _, msgGeneric := range outboundStream.batchMessages {
		// sm.logger.Infof("Message %d of type %s in batch sent to %s", idx, reflect.TypeOf(msgGeneric.msg), outboundStream.targetPeer.String())
		sm.babel.MessageDelivered(msgGeneric.originProto, msgGeneric.msg, outboundStream.targetPeer)
	}
	outboundStream.batchBytes = make([]byte, 0, sm.conf.BatchMaxSizeBytes)
	outboundStream.batchMessages = make([]struct {
		originProto uint16
		msg         message.Message
	}, 0)

	select {
	case outboundStream.batchFlush <- time.Now():
	default:
	}

	return nil
}

func (sm *babelStreamManager) Disconnect(disconnectingProto protocol.ID, p peer.Peer) {
	k := getKeyForConn(disconnectingProto, p)
	outboundStreamInt, loaded := sm.outboundTransports.LoadAndDelete(k)
	if loaded {
		outboundStream := outboundStreamInt.(*outboundTransport)
		sm.closeConn(outboundStream.conn)
		sm.logStreamManagerState()
		outboundStream.connMU.Lock()
		select {
		case <-outboundStream.Finished:
		default:
			close(outboundStream.Finished)
		}
		outboundStream.connMU.Unlock()
		outboundStream.batchMU.Lock()
		for _, msgGeneric := range outboundStream.batchMessages {
			sm.babel.MessageDeliveryErr(msgGeneric.originProto,
				msgGeneric.msg,
				outboundStream.targetPeer,
				errors.NonFatalError(500, "disconnected from peer meanwhile", streamManagerCaller))
		}
		outboundStream.batchBytes = make([]byte, 0)
		outboundStream.batchMessages = make([]struct {
			originProto uint16
			msg         message.Message
		}, 0)
		outboundStream.batchMU.Unlock()
	}
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
			sm.closeConn(mr)
			mr = nil
			sm.inboundTransports.Delete(newPeer.String())
			return
		}

		if len(msgBuf) == 0 {
			panic("got 0 bytes from conn")
		}

		for i := 0; i < len(msgBuf); {
			msgSize := binary.BigEndian.Uint32(msgBuf[i : i+4])
			i += 4
			if msgSize == 0 && len(msgBuf) > i+int(msgSize) {
				sm.logger.Panicf("Msg size is %d and i: %d but still have %d bytes total to read from batch", msgSize, i, len(msgBuf)-i)
			}

			msgGeneric := deserializer.Deserialize(msgBuf[i : i+int(msgSize)])
			msgWrapper := msgGeneric.(*internalMsg.AppMessageWrapper)
			appMsg := sm.babel.SerializationManager().Deserialize(msgWrapper.MessageID, msgWrapper.WrappedMsgBytes)
			sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
			i += int(msgSize)
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
	sm.closeConn(c)
}

func (sm *babelStreamManager) waitForHandshakeMessage(frameBasedConn messageIO.FrameConn) (
	*internalMsg.ProtoHandshakeMessage,
	errors.Error,
) {
	msgBytes, err := frameBasedConn.ReadFrame()
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	msg := protoMsgSerializer.Deserialize(msgBytes).(internalMsg.ProtoHandshakeMessage)
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return &msg, nil
}

func (sm *babelStreamManager) sendHandshakeMessage(
	transport messageIO.FrameConn,
	dialerProto protocol.ID,
	chanType uint8,
) errors.Error {
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
