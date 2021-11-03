package pkg

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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
	// "github.com/smallnest/goframe"
)

const (
	MaxTCPFrameSize   = 65535
	MaxUDPMsgSize     = 65535
	WriteChanCapacity = 10_000_000
)

var (
	ErrConnectionClosed = fmt.Errorf("connection closed")
	// streamManagerEncoderConfig = goframe.EncoderConfig{
	// 	ByteOrder:                       binary.BigEndian,
	// 	LengthFieldLength:               4,
	// 	LengthAdjustment:                0,
	// 	LengthIncludesLengthFieldLength: false,
	// }

	// streamManagerDecoderConfig = goframe.DecoderConfig{
	// 	ByteOrder:           binary.BigEndian,
	// 	LengthFieldOffset:   0,
	// 	LengthFieldLength:   4,
	// 	LengthAdjustment:    0,
	// 	InitialBytesToStrip: 4,
	// }
)

type StreamManagerConf struct {
	BatchMaxSizeBytes int
	BatchTimeout      time.Duration
	DialTimeout       time.Duration
	WriteTimeout      time.Duration
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
	buf       *bytes.Buffer
	Callbacks []func(error)
	sm        *babelStreamManager
}

func (b *batch) addFrameToBatch(cb func(error), frame []byte) {
	b.Callbacks = append(b.Callbacks, cb)
	if err := b.sm.writeFrame(b.buf, frame); err != nil {
		b.sm.logger.Panic(err.Error())
	}
}

type outboundTransport struct {
	k    int
	Addr net.Addr

	logger *log.Entry

	Dialed     chan interface{}
	DialErr    chan interface{}
	Finished   chan interface{}
	writersMux *sync.Mutex
	writers    *sync.WaitGroup
	ToWrite    chan *batch

	batchMu  *sync.Mutex
	conn     io.Closer
	writeBuf io.Writer

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
		ot.batchMu.Lock()
		defer ot.batchMu.Unlock()
		ot.flushBatch()
		nextDeadline := time.Now().Add(ot.sm.conf.BatchTimeout)
		return true, &nextDeadline
	}
}

func (ot *outboundTransport) flushBatch() {
	defer func() {
		if x := recover(); x != nil {
			ot.logger.Panicf("Panic in flush batch: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()
	// ot.logger.Infof("Flushing batch...")

	if ot.batch.buf.Len() > 0 {
		ot.sendBatchToChan(ot.batch)
		ot.batch = &batch{
			buf:       new(bytes.Buffer),
			Callbacks: []func(error){},
			sm:        ot.sm,
		}
	}
}

func (ot *outboundTransport) sendBatchToChan(b *batch) {
	ot.writersMux.Lock()
	ot.writers.Add(1)
	ot.writersMux.Unlock()
	select {
	case <-ot.Finished:
		for _, cb := range b.Callbacks {
			cb(ErrConnectionClosed)
		}
		ot.writers.Done()
		return
	default:
	}

	select {
	case ot.ToWrite <- b:
		ot.writers.Done()
		return
	default:
		ot.logger.Info("Sending message detached")
		go func() {
			ot.ToWrite <- b
			ot.writers.Done()
		}()
	}
}

func (ot *outboundTransport) writeToConn(batch *batch) error {
	_, err := io.CopyN(ot.writeBuf, batch.buf, int64(batch.buf.Len()))
	for _, cb := range batch.Callbacks {
		cb(err)
	}
	if err != nil {
		ot.logger.Errorf("Out connection got unexpected error: %s", err.Error())
	}
	return err
}

func (ot *outboundTransport) writeOutboundMessages() {
	defer ot.logger.Infof("Out connection exited...")

	var connErr error

	for batch := range ot.ToWrite {
		if connErr != nil {
			for _, cb := range batch.Callbacks {
				cb(connErr)
			}
		}
		connErr = ot.writeToConn(batch)
		if connErr != nil {
			go ot.close(connErr)
		}
	}
	// ot.
	ot.sm.closeConn(ot.conn)
}

func (ot *outboundTransport) sendMessage(originProto, destProto protocol.ID, toSend message.Message,
	dest peer.Peer, batchMessage, canDetach bool) {

	defer func() {
		if x := recover(); x != nil {
			ot.logger.Panicf("Panic in sendMessage: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()

	if canDetach {
		select {
		case <-ot.DialErr:
		case <-ot.Dialed:
		default:
			go ot.sendMessage(originProto, destProto, toSend, dest, batchMessage, false)
			return
		}
	}

	select {
	case <-ot.DialErr:
		ot.sm.babel.MessageDeliveryErr(originProto, toSend, dest, errors.NonFatalError(500, "dial failed", streamManagerCaller))
		return
	case <-ot.Finished:
		ot.sm.babel.MessageDeliveryErr(originProto, toSend, dest, errors.NonFatalError(500, "stream finished", streamManagerCaller))
		return
	case <-ot.Dialed:
		internalMsgBytes, _ := ot.sm.babel.SerializationManager().Serialize(toSend)
		msgBytes := internalMsg.NewAppMessageWrapper(
			toSend.Type(),
			originProto,
			destProto,
			ot.sm.babel.SelfPeer(),
			internalMsgBytes,
		).Serialize()
		if batchMessage {
			ot.writersMux.Lock()
			ot.writers.Add(1)
			ot.writersMux.Unlock()
			select {
			case <-ot.Finished:
				ot.sm.babel.MessageDeliveryErr(originProto, toSend, dest, errors.NonFatalError(500, ErrConnectionClosed.Error(), streamManagerCaller))
				ot.writers.Done()
				return
			default:
				ot.batchMu.Lock()
				defer ot.batchMu.Unlock()
				if ot.batch.buf.Len() >= ot.sm.conf.BatchMaxSizeBytes {
					ot.flushBatch()
				}
				ot.batch.addFrameToBatch(func(err error) {
					ot.messageCallback(err, toSend, originProto)
				}, msgBytes)
				ot.writers.Done()
			}
		} else {
			tmpBatch := &batch{
				buf: new(bytes.Buffer),
				sm:  ot.sm,
			}
			tmpBatch.addFrameToBatch(func(err error) {
				ot.messageCallback(err, toSend, originProto)
			}, msgBytes)
			ot.sendBatchToChan(tmpBatch)
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

func (ot *outboundTransport) close(err error) {
	ot.closeOnce.Do(func() {
		ot.sm.outboundTransports.Delete(ot.targetPeer.String())
		close(ot.Finished)
		// wait for writers
		ot.writersMux.Lock()
		ot.logger.Info("Waiting for writers")
		ot.writers.Wait()
		ot.logger.Info("Waited for writers")
		ot.writersMux.Unlock()

		// all writers are done, finish flushing batch
		ot.logger.Info("Closed finished chan")
		ot.batchMu.Lock()
		ot.logger.Info("Flushing batch messages")
		ot.flushBatch()
		ot.batchMu.Unlock()
		ot.logger.Info("Flushed batch messages")

		close(ot.ToWrite)
		ot.logger.Info("Closed write channel")
		if err != nil {
			ot.logger.Info("Delivering OutTransportFailure")
			ot.sm.babel.OutTransportFailure(ot.originProto, ot.targetPeer)
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
		for range time.NewTicker(5 * time.Second).C {
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

		// conn := gev.
		// frameBasedConn := goframe.NewLengthFieldBasedFrameConn(streamManagerEncoderConfig, streamManagerDecoderConfig, tcpStream)
		hErr := sm.sendHandshakeMessage(tcpStream, sourceProtoID, TemporaryTunnel)
		if hErr != nil {
			tmpErr := errors.NonFatalError(500, err.Error(), streamManagerCaller)
			sm.babel.MessageDeliveryErr(sourceProtoID, toSend, peer, tmpErr)
			return tmpErr
		}
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := internalMsg.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProto, sm.babel.SelfPeer(), msgBytes)
		wErr := sm.writeFrame(tcpStream, msgWrapper.Serialize())
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

func (sm *babelStreamManager) closeConn(c io.Closer) {
	err := c.Close()
	if err != nil {
		sm.logger.Errorf("Err closing connection: %s", err.Error())
	}
}

func (sm *babelStreamManager) AcceptConnectionsAndNotify(lAddrInt net.Addr) chan interface{} {
	done := make(chan interface{})

	go func() {
		sm.logger.Infof("Starting listener of type %s on addr: %s", lAddrInt.Network(), lAddrInt.String())

		defer func() {
			if x := recover(); x != nil {
				sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))
			}
		}()

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
				err = newStream.(*net.TCPConn).SetReadBuffer(MaxTCPFrameSize)
				if err != nil {
					sm.logger.Panic(err.Error())
				}
				err = newStream.(*net.TCPConn).SetWriteBuffer(MaxTCPFrameSize)
				if err != nil {
					sm.logger.Panic(err.Error())
				}

				go func() {
					defer func() {
						if x := recover(); x != nil {
							sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))
						}
					}()
					// frameBasedConn := goframe.NewLengthFieldBasedFrameConn(streamManagerEncoderConfig, streamManagerDecoderConfig, newStream)

					newStreamReader := bufio.NewReaderSize(newStream, MaxTCPFrameSize)
					defer newStreamReader.Discard(newStreamReader.Buffered())
					handshakeMsg, err := sm.waitForHandshakeMessage(newStream, newStreamReader)
					if err != nil {
						sm.closeConn(newStream)
						err.Log(sm.logger)
						sm.logStreamManagerState()
						return
					}

					defer sm.closeConn(newStream)
					remotePeer := handshakeMsg.Peer
					if handshakeMsg.TunnelType == TemporaryTunnel {
						sm.handleTmpStream(remotePeer, newStreamReader)
						return
					}

					go sm.babel.InConnRequested(handshakeMsg.DialerProto, remotePeer)
					// sm.logger.Infof("Accepted connection from %s successfully", remotePeer.String())
					sm.inboundTransports.Store(remotePeer.String(), newStream)
					sm.logStreamManagerState()
					sm.handleInStream(newStreamReader, remotePeer)
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
		logger:      sm.logger.WithField("peer", toDial.String()),
		Dialed:      make(chan interface{}),
		DialErr:     make(chan interface{}),
		Finished:    make(chan interface{}),
		writersMux:  &sync.Mutex{},
		writers:     &sync.WaitGroup{},
		ToWrite:     make(chan *batch, WriteChanCapacity),
		conn:        nil,
		batchMu:     &sync.Mutex{},
		targetPeer:  toDial,
		originProto: dialingProto,
		batch: &batch{
			buf:       new(bytes.Buffer),
			Callbacks: []func(error){},
			sm:        sm,
		},
		babel:     sm.babel,
		sm:        sm,
		closeOnce: &sync.Once{},
	}
	_, loaded := sm.outboundTransports.LoadOrStore(k, newOutboundTransport)
	if loaded {
		sm.logger.Warnf("Stream already exists", toDial.String())
		return errors.NonFatalError(500, "Stream already exists", streamManagerCaller)
	}
	go func() {
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
		if newStreamTyped, ok := conn.(*net.TCPConn); ok {
			err = newStreamTyped.SetReadBuffer(MaxTCPFrameSize)
			if err != nil {
				sm.logger.Panic(err.Error())
			}
			err = newStreamTyped.SetWriteBuffer(MaxTCPFrameSize)
			if err != nil {
				sm.logger.Panic(err.Error())
			}
		}

		switch newStreamTyped := conn.(type) {
		case net.Conn:
			// frameBasedConn := goframe.NewLengthFieldBasedFrameConn(streamManagerEncoderConfig, streamManagerDecoderConfig, newStreamTyped)
			newOutboundTransport.conn = newStreamTyped
			newOutboundTransport.writeBuf = bufio.NewWriterSize(newStreamTyped, MaxTCPFrameSize)
			herr := sm.sendHandshakeMessage(newStreamTyped, dialingProto, PermanentTunnel)
			if herr != nil {
				sm.logger.Errorf("An error occurred during handshake with %s: %s", toDial.String(), herr)
				sm.closeConn(newStreamTyped)
				close(newOutboundTransport.DialErr)
				sm.babel.DialError(dialingProto, toDial)
				sm.outboundTransports.Delete(k)
				return
			}

			if !sm.babel.DialSuccess(dialingProto, toDial) {
				sm.logger.Error("protocol did not accept conn")
				sm.closeConn(newStreamTyped)
				close(newOutboundTransport.DialErr)
				sm.outboundTransports.Delete(k)
				return
			}

			close(newOutboundTransport.Dialed)
			sm.logger.Infof("Dialed %s successfully", k)
			sm.teq.Add(newOutboundTransport, time.Now().Add(sm.conf.BatchTimeout))
			sm.logStreamManagerState()
			newOutboundTransport.writeOutboundMessages()
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
	outboundStreamInt.(*outboundTransport).sendMessage(origin, destination, toSend, destPeer, batch, true)
}

func (sm *babelStreamManager) Disconnect(disconnectingProto protocol.ID, p peer.Peer) {
	defer func() {
		if x := recover(); x != nil {
			sm.logger.Panicf("run time PANIC: %v, STACK: %s", x, string(debug.Stack()))
		}
	}()
	sm.logger.Tracef("disconnecting from peer %s", p.String())
	defer sm.logger.Tracef("Done disconnecting from peer %s", p.String())

	k := GetKeyForConn(disconnectingProto, p)
	outboundStreamInt, loaded := sm.outboundTransports.LoadAndDelete(k)
	if loaded {
		go outboundStreamInt.(*outboundTransport).close(nil)
	}
}

func (sm *babelStreamManager) DisconnectInStream(disconnectingProto protocol.ID, p peer.Peer) {

	stream, ok := sm.inboundTransports.Load(p.String())
	if !ok {
		return
	}
	sm.closeConn(stream.(net.Conn))
}

func (sm *babelStreamManager) writeFrame(conn io.Writer, frame []byte) error {
	nrBytesToWrite := len(frame)
	msgLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLenBytes, uint32(nrBytesToWrite))
	msgLenBytes = append(msgLenBytes, frame...)

	curr := 0
	for curr != len(msgLenBytes) {
		written, err := conn.Write(msgLenBytes[curr:])
		curr += written
		if err == io.EOF {
			return err
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (sm *babelStreamManager) readFrame(reader io.Reader) ([]byte, error) {
	var readErr error

	var nrBytesInMsg uint32

	msgSizeBytes := make([]byte, 4)
	n, readErr := io.ReadFull(reader, msgSizeBytes)
	if readErr != nil {
		return nil, readErr
	}
	if n != 4 {
		sm.logger.Panic("Did not read enough bytes ")
	}
	nrBytesInMsg = binary.BigEndian.Uint32(msgSizeBytes)
	if nrBytesInMsg == 0 {
		sm.logger.Errorf("Reading frame with 0 bytes")
		return nil, fmt.Errorf("0 byte frame received")
	}
	// sm.logger.Infof("Reading frame with %d bytes", nrBytesInMsg)
	msgBytes := make([]byte, nrBytesInMsg)
	// defer sm.logger.Infof("Frame bytes: %s", msgBytes)
	n, readErr = io.ReadFull(reader, msgBytes)
	if readErr != nil {
		if readErr == io.EOF {
			if len(msgBytes) != n {
				sm.logger.Error("Frame size does not match message size")
				return nil, readErr
			}
			return msgBytes[:n], nil
		}
		return nil, readErr
	}
	return msgBytes[:n], readErr
}

func (sm *babelStreamManager) handleInStream(conn io.Reader, newPeer peer.Peer) {
	logger := sm.logger.WithField("inStream", newPeer.String())

	logger.Info("[ConnectionEvent] : Started handling peer stream")
	defer logger.Info("[ConnectionEvent] : Done handling peer stream")
	sm.logStreamManagerState()
	for {
		frame, readErr := sm.readFrame(conn)
		if len(frame) > 0 {
			msgWrapper := &internalMsg.AppMessageWrapper{}
			_, err := msgWrapper.Deserialize(frame)
			if err != nil {
				sm.logger.Panic(err.Error())
				// if err.Error() == internalMsg.ErrNotEnoughLen.Error() {
				// 	carry = newFrame
				// 	break
				// }
			}
			appMsg, err := sm.babel.SerializationManager().Deserialize(msgWrapper.MessageID, msgWrapper.WrappedMsgBytes)
			if err != nil {
				logger.Errorf("Got error %s deserializing message of type %d from %s", err.Error(), msgWrapper.MessageID, newPeer)
				continue
			}
			sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
			sm.addMsgReceived(msgWrapper.DestProto)
		}
		if readErr != nil {
			logger.Errorf("Read routine got error: %s", readErr.Error())
			return
		}

	}
}

func (sm *babelStreamManager) handleTmpStream(newPeer peer.Peer, c io.Reader) {
	if peer.PeersEqual(newPeer, sm.babel.SelfPeer()) {
		sm.logger.Panic("Dialing self")
	}

	//sm.logger.Info("Reading from tmp stream")
	frame, connErr := sm.readFrame(c)
	msgWrapper := &internalMsg.AppMessageWrapper{}
	if len(frame) > 0 {
		_, err := msgWrapper.Deserialize(frame)
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
		sm.logger.Errorf("%+v", msgWrapper)
		sm.logger.Errorf("%+v\n %s", frame, frame)
		sm.logger.Errorf("%+v", appMsg)
		sm.logger.Errorf("Got error %s deserializing message of type %d from %s", err.Error(), msgWrapper.MessageID, newPeer)
		return
	}
	sm.babel.DeliverMessage(newPeer, appMsg, msgWrapper.DestProto)
	sm.addMsgReceived(msgWrapper.DestProto)
}

func (sm *babelStreamManager) waitForHandshakeMessage(conn net.Conn, reader io.Reader) (
	*internalMsg.ProtoHandshakeMessage,
	errors.Error,
) {
	conn.SetReadDeadline(time.Now().Add(sm.conf.DialTimeout))
	defer conn.SetReadDeadline(time.Time{})
	msgBuf, err := sm.readFrame(reader)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	handshakeMsg := &internalMsg.ProtoHandshakeMessage{}
	err = handshakeMsg.Deserialize(msgBuf)
	if err != nil {
		return nil, errors.NonFatalError(500, err.Error(), streamManagerCaller)
	}
	sm.logger.Infof("Received handshake message: %+v", handshakeMsg)
	//sm.logger.Infof("Received proto exchange message %+v", msg)
	return handshakeMsg, nil
}

func (sm *babelStreamManager) sendHandshakeMessage(
	transport io.Writer,
	dialerProto protocol.ID,
	chanType uint8,
) error {
	var toSendMsg = internalMsg.NewProtoHandshakeMessage(dialerProto, sm.babel.SelfPeer(), chanType)
	toSendBytes := toSendMsg.Serialize()
	return sm.writeFrame(transport, toSendBytes)
}

func GetKeyForConn(protoId protocol.ID, peer peer.Peer) string {
	return peer.String()
}

func (sm *babelStreamManager) logStreamManagerState() {
	toLog := "inbound connections : "
	sm.inboundTransports.Range(
		func(peer, conn interface{}) bool {
			toLog += fmt.Sprintf("%s, ", peer.(string))
			return true
		},
	)
	sm.logger.Info(toLog)
	toLog = "outbound connections : "
	sm.outboundTransports.Range(
		func(peer, conn interface{}) bool {
			toLog += fmt.Sprintf("%s, ", peer.(string))
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
