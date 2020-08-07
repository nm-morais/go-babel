package pkg

import (
	"fmt"
	"github.com/nm-morais/go-babel/configs"
	"github.com/nm-morais/go-babel/internal/notificationHub"
	internalProto "github.com/nm-morais/go-babel/internal/protocol"
	"github.com/nm-morais/go-babel/internal/serialization"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/handlers"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/notification"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/request"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/nm-morais/go-babel/pkg/transport"
	log "github.com/sirupsen/logrus"
	"reflect"
	"sync"
	"time"
)

const configFilePath = "./configs/config.json"

const ProtoManagerCaller = "ProtoManager"

/*
type IProtocolManager interface {
	RegisteredProtos() []protocol.ID
	RegisterProtocol(protocol protocol.Protocol) Error
	RegisterRequestHandler(protocol protocol.ID, request request.ID, handler handlers.RequestHandler) Error
	RegisterMessageHandler(protocol protocol.ID, request message.ID, handler handlers.MessageHandler) Error
	ReceiveMessage(message message.Message, peer peer.Peer)
	SendMessage(message message.Message, peer peer.Peer, origin protocol.ID, destinations []protocol.ID) Error
	SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) Error
	Dial(peer peer.Peer, sourceProto protocol.ID, transport transport.Transport) Error
	dialFailed(peer peer.Peer, sourceProto protocol.ID)
	dialSuccess(peer peer.Peer, sourceProto protocol.ID)
	inConnRequested(peerProtos []protocol.ID, peer peer.Peer, transport transport.Transport) bool
	transportFailure(peerProtos []protocol.ID, transport transport.Transport)
}
*/

type protocolValueType = *internalProto.WrapperProtocol
type activeTransportValueType = transport.Transport

var hbProtoInternalID = protocol.ID(1)
var reservedProtos = []protocol.ID{hbProtoInternalID}

type protoManager struct {
	selfPeer                peer.Peer
	config                  configs.ProtocolManagerConfig
	notificationHub         notificationHub.NotificationHub
	hbChannels              *sync.Map
	serializationManager    *serialization.Manager
	protocols               *sync.Map
	protoIds                []protocol.ID
	dialingTransports       *sync.Map
	dialingTransportsMutex  *sync.Mutex
	activeTransports        *sync.Map
	listener                transport.Transport
	channelSubscribers      map[string]map[protocol.ID]bool
	channelSubscribersMutex *sync.Mutex
}

var p *protoManager
var protoMsgSerializer = message.ProtoHandshakeMessageSerializer{}

func GetProtocolManager() *protoManager {
	return p
}

func InitProtoManager(configs configs.ProtocolManagerConfig) *protoManager {
	p = &protoManager{
		config:                  configs,
		hbChannels:              &sync.Map{},
		selfPeer:                peer.NewPeer(configs.ListenAddr),
		notificationHub:         notificationHub.NewNotificationHub(),
		serializationManager:    serialization.NewSerializationManager(),
		protocols:               &sync.Map{},
		protoIds:                []protocol.ID{},
		dialingTransports:       &sync.Map{},
		dialingTransportsMutex:  &sync.Mutex{},
		activeTransports:        &sync.Map{},
		channelSubscribers:      make(map[string]map[protocol.ID]bool),
		channelSubscribersMutex: &sync.Mutex{},
	}
	p.serializationManager.RegisterSerializer(message.HeartbeatMessageType, message.HeartbeatSerializer{})
	return p
}

func handleTransportListener() {
	transports := p.listener.Listen()
	for newPeerTransport := range transports {
		go newPeerTransport.PipeBytesToChan()

		handshakeMsg, err := exchangeHandshakeMessage(newPeerTransport, p.protoIds, true)
		if err != nil {
			newPeerTransport.Close()
			return
		}
		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)

		if handshakeMsg.TemporaryConn == 1 {
			go handleIncTmpTransport(remotePeer, newPeerTransport)
			continue
		}

		p.channelSubscribersMutex.Lock()
		subs := p.channelSubscribers[remotePeer.ToString()]
		for _, remoteProtoID := range handshakeMsg.Protos {
			if proto, ok := p.protocols.Load(remoteProtoID); ok {
				if proto.(protocolValueType).InConnRequested(remotePeer) {
					if subs == nil {
						subs = make(map[protocol.ID]bool)
					}
					subs[proto.(protocolValueType).ID()] = true
				}
			}
		}
		p.channelSubscribers[remotePeer.ToString()] = subs
		if len(p.channelSubscribers[remotePeer.ToString()]) == 0 {
			newPeerTransport.Close()
			p.channelSubscribersMutex.Unlock()
			return
		}
		p.channelSubscribersMutex.Unlock()
		p.activeTransports.Store(remotePeer.ToString(), newPeerTransport)
		go handlePeerConn(newPeerTransport, remotePeer)
	}
}

func RegisterTransportListener(listener transport.Transport) {
	p.listener = listener
}

func RegisterProtocol(protocol protocol.Protocol) errors.Error {
	_, ok := p.protocols.Load(protocol.ID())
	for _, protoID := range reservedProtos {
		if protocol.ID() == protoID {
			log.Panicf("Trying to add protocol with invalid ID (reserved by internal mechanisms). Reserved protos: %+v", reservedProtos)
		}
	}

	if ok {
		return errors.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
	log.Infof("Protocol %s registered", reflect.TypeOf(protocol))
	protocolWrapper := internalProto.NewWrapperProtocol(protocol)
	p.protocols.Store(protocol.ID(), protocolWrapper)
	p.protoIds = append(p.protoIds, protocol.ID())
	return nil
}

func RegisterNotificationHandler(protoID protocol.ID, notificationID notification.ID, handler handlers.NotificationHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if ok {
		return errors.FatalError(409, "Protocol already registered", ProtoManagerCaller)
	}
	p.notificationHub.AddListener(notificationID, proto.(protocolValueType))
	proto.(protocolValueType).RegisterNotificationHandler(notificationID, handler)
	return nil
}

func RegisterTimerHandler(protoID protocol.ID, timer timer.ID, handler handlers.TimerHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterTimerHandler(timer, handler)
	return nil
}

func RegisterRequestHandler(protoID protocol.ID, request request.ID, handler handlers.RequestHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).RegisterRequestHandler(request, handler)
	return nil
}

func RegisterMessageHandler(protoID protocol.ID, message message.Message, handler handlers.MessageHandler) errors.Error {
	proto, ok := p.protocols.Load(protoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	log.Infof("Protocol %d registered handler for msg %+v", protoID, reflect.TypeOf(message))
	p.serializationManager.RegisterSerializer(message.Type(), message.Serializer())
	p.serializationManager.RegisterDeserializer(message.Type(), message.Deserializer())

	proto.(protocolValueType).RegisterMessageHandler(message.Type(), handler)
	return nil
}

func SendMessage(toSend message.Message, destPeer peer.Peer, origin protocol.ID, destinations []protocol.ID) errors.Error {
	conn, ok := p.activeTransports.Load(destPeer.ToString())
	if !ok {
		return errors.NonFatalError(404, fmt.Sprintf("No active connection to peer %s", destPeer.ToString()), ProtoManagerCaller)
	}
	//log.Infof("Sending message of type %s", reflect.TypeOf(toSend))
	go func() {
		msgBytes := p.serializationManager.Serialize(toSend)
		wrapper := message.NewAppMessageWrapper(toSend.Type(), origin, destinations, msgBytes)
		// log.Infof("Sending: %+v", wrapper)
		toSendBytes := wrapper.Serializer().Serialize(wrapper)
		// log.Infof("Sending (bytes): %+v", toSendBytes)
		err := conn.(transport.Transport).SendMessage(toSendBytes)
		if err != nil {
			err.Log()
			conn.(transport.Transport).Close()
		}
		//log.Info("renewing hb timer")
		renewHbTimer(destPeer)
		//log.Info("done renewing  hb timer")
	}()

	return nil
}

func SendRequest(request request.Request, origin protocol.ID, destination protocol.ID) errors.Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}

	go func() {
		respChan := proto.(protocolValueType).DeliverRequest(request)
		reply := <-respChan
		SendRequestReply(reply, destination, origin)
	}()
	return nil
}

func SendRequestReply(reply request.Reply, origin protocol.ID, destination protocol.ID) errors.Error {
	_, ok := p.protocols.Load(origin)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto, ok := p.protocols.Load(destination)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	proto.(protocolValueType).DeliverRequestReply(reply)
	return nil
}

func SendNotification(notification notification.Notification) errors.Error {
	p.notificationHub.AddNotification(notification)
	return nil
}

func RegisterTimer(origin protocol.ID, timer timer.Timer) errors.Error {
	callerProto, ok := p.protocols.Load(origin)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	go func() { // TODO can be improved to use single routine instead of routine per channel
		timer.Wait()
		callerProto.(protocolValueType).DeliverTimer(timer)
	}()
	return nil
}

func RegisteredProtos() []protocol.ID {
	return p.protoIds
}

func SendMessageTempTransport(toSend message.Message, targetPeer peer.Peer, sourceProtoID protocol.ID, destProtos []protocol.ID, t transport.Transport) {
	errChan := t.Dial(targetPeer)
	go func() {
		err := <-errChan
		if err != nil {
			log.Error(err)
			return
		}
		go t.PipeBytesToChan()
		sendHandshakeMessage(t, destProtos, true)
		msgBytes := toSend.Serializer().Serialize(toSend)
		msgWrapper := message.NewAppMessageWrapper(toSend.Type(), sourceProtoID, destProtos, msgBytes)
		// log.Info("Sending message sideChannel")
		wrappedBytes := msgWrapper.Serializer().Serialize(msgWrapper)
		t.SendMessage(wrappedBytes)
		t.Close()
	}()
}

func handleIncTmpTransport(newPeer peer.Peer, transport transport.Transport) {
	deserializer := message.AppMessageWrapperSerializer{}
	msgChan := transport.MessageChan()
	// log.Info("Waiting for message on temp channel...")
	msg, ok := <-msgChan
	if !ok {
		log.Error("temporary conn exited before receiving applicational message")
		return
	}
	// log.Info("received message via temp channel")
	deserialized := deserializer.Deserialize(msg)
	protoMsg := deserialized.(*message.AppMessageWrapper)
	appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
	for _, toNotifyID := range protoMsg.DestProtos {
		if toNotify, ok := p.protocols.Load(toNotifyID); ok {
			toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
		} else {
			log.Errorf("Ignored message: %+v", protoMsg)
		}
	}
	transport.Close()
}

func Dial(toDial peer.Peer, sourceProtoID protocol.ID, t transport.Transport) errors.Error {
	log.Infof("Dialing new node %s", toDial.Addr())

	sourceProto, ok := p.protocols.Load(sourceProtoID)
	if !ok {
		return errors.FatalError(409, "Protocol not registered", ProtoManagerCaller)
	}
	decodedSourceProto := sourceProto.(protocolValueType)

	_, ok = p.activeTransports.Load(toDial.ToString())
	if ok {
		p.channelSubscribersMutex.Lock()
		if decodedSourceProto.DialSuccess(sourceProtoID, toDial) {
			subs := p.channelSubscribers[toDial.ToString()]
			if subs == nil {
				subs = make(map[protocol.ID]bool)
			}
			subs[decodedSourceProto.ID()] = true
			p.channelSubscribers[toDial.ToString()] = subs
		}
		p.channelSubscribersMutex.Unlock()
		return nil
	}

	doneDialing, ok := p.dialingTransports.Load(toDial.ToString())
	if ok {
		waitChan := doneDialing.(chan interface{})
		go waitDial(sourceProtoID, toDial, waitChan)
		return nil
	}

	p.dialingTransportsMutex.Lock()
	doneDialing, ok = p.dialingTransports.Load(toDial.ToString())
	if ok {
		waitChan := doneDialing.(chan interface{})
		go waitDial(sourceProtoID, toDial, waitChan)
		p.dialingTransportsMutex.Unlock()
		return nil
	}
	done := make(chan interface{})
	p.dialingTransports.Store(toDial.ToString(), done)
	p.dialingTransportsMutex.Unlock()

	go func() {
		errChan := t.Dial(toDial)
		defer func() {
			close(done)
			p.dialingTransports.Delete(toDial.ToString())
		}()

		err := <-errChan
		if err != nil {
			err.Log()
			decodedSourceProto.DialFailed(toDial)
			return
		}
		go t.PipeBytesToChan()

		// log.Infof("Done dialing node %s", toDial.Addr())
		// log.Infof("Exchanging protos")
		//log.Info("Remote protos: %d", handshakeMsg.Protos)

		handshakeMsg, err := exchangeHandshakeMessage(t, p.protoIds, false)
		if err != nil {
			t.Close()
			return
		}

		remotePeer := peer.NewPeer(handshakeMsg.ListenAddr)
		p.channelSubscribersMutex.Lock()
		for _, destProtoID := range handshakeMsg.Protos {
			proto, ok := p.protocols.Load(destProtoID)
			if ok {
				convertedProto := proto.(protocolValueType)
				if convertedProto.DialSuccess(sourceProtoID, remotePeer) {
					subs := p.channelSubscribers[toDial.ToString()]
					if subs == nil {
						subs = make(map[protocol.ID]bool)
					}
					subs[convertedProto.ID()] = true
					p.channelSubscribers[toDial.ToString()] = subs
				}
			}
		}
		if len(p.channelSubscribers[remotePeer.ToString()]) == 0 {
			t.Close()
			p.channelSubscribersMutex.Unlock()
			return
		}
		p.channelSubscribersMutex.Unlock()
		p.activeTransports.Store(remotePeer.ToString(), t)
		go handlePeerConn(t, remotePeer)
	}()
	return nil
}

func Disconnect(source protocol.ID, peer peer.Peer) {
	log.Warnf("Disconnecting from %s", peer.ToString())
	p.channelSubscribersMutex.Lock()
	subscribers := p.channelSubscribers[peer.ToString()]
	delete(subscribers, source)
	if len(subscribers) == 0 {
		if t, ok := p.activeTransports.Load(peer.ToString()); ok {
			t.(activeTransportValueType).Close()
			p.activeTransports.Delete(peer.ToString())
		}
		delete(subscribers, source)
	}
	p.channelSubscribersMutex.Unlock()
}

func SelfPeer() peer.Peer {
	return p.selfPeer
}

func handlePeerConn(t transport.Transport, newPeer peer.Peer) {

	if len(p.channelSubscribers[newPeer.ToString()]) == 0 {
		t.Close()
		p.activeTransports.Delete(newPeer.ToString())
		p.dialingTransports.Delete(newPeer.ToString())
		log.Panic("Dialed node has no interested protocols")
	}

	go startConnHeartbeat(newPeer)
	deserializer := message.AppMessageWrapperSerializer{}
	for {
		t.SetReadTimeout(p.config.ConnectionReadTimeout)
		msgChan := t.MessageChan()
		select {
		case msg, ok := <-msgChan:

			if !ok || len(msg) == 0 {
				handleTransportFailure(newPeer)
				return
			}

			deserialized := deserializer.Deserialize(msg)
			protoMsg := deserialized.(*message.AppMessageWrapper)

			if protoMsg.MessageID == message.HeartbeatMessageType {
				//log.Warnf("Got heartbeat from %s", newPeer.ToString())
				continue
			}

			// log.Warnf("Got message: %+v", protoMsg)

			//log.Infof("Got protoMessage: %+v", protoMsg)
			for _, toNotifyID := range protoMsg.DestProtos {
				if toNotify, ok := p.protocols.Load(toNotifyID); ok {
					appMsg := p.serializationManager.Deserialize(protoMsg.MessageID, protoMsg.WrappedMsgBytes)
					log.Warnf("Got message: %s", reflect.TypeOf(appMsg))
					toNotify.(protocolValueType).DeliverMessage(newPeer, appMsg)
				} else {
					log.Panicf("Ignored message: %+v", protoMsg)
				}
			}
		case <-time.After(3 * time.Second):
			panic("Did not receive message for 3 seconds")
		}
	}
}

func startConnHeartbeat(peer peer.Peer) {
	hbChannel := make(chan *struct{})
	p.hbChannels.Store(peer, hbChannel)
	defer func() {
		close(hbChannel)
		p.hbChannels.Delete(peer)
		log.Warn("Heartbeat routine exiting...")
	}()
	hbMessage := message.HeartbeatMessage{}
	for {
		select {
		case _, ok := <-hbChannel:
			if !ok {
				return
			}
		case <-time.After(p.config.HeartbeatTickDuration):
			if err := SendMessage(hbMessage, peer, hbProtoInternalID, []protocol.ID{hbProtoInternalID}); err != nil {
				err.Log()
				return
			}
		}
	}
}

func renewHbTimer(peer peer.Peer) {
	if hbChan, ok := p.hbChannels.Load(peer); ok {
		select {
		case hbChan.(chan *struct{}) <- nil:
		case <-time.After(3 * time.Second):
			log.Panicf("Could not renew HB timer")
		}
	}
}

func handleTransportFailure(peer peer.Peer) {
	p.channelSubscribersMutex.Lock()
	toNotify := p.channelSubscribers[peer.ToString()]
	for protoID, _ := range toNotify {
		proto, _ := p.protocols.Load(protoID)
		proto.(protocolValueType).TransportFailure(peer)
	}
	delete(p.channelSubscribers, peer.ToString())
	p.channelSubscribersMutex.Unlock()
	p.dialingTransportsMutex.Lock()
	p.activeTransports.Delete(peer.ToString())
	p.dialingTransportsMutex.Unlock()
}

func exchangeHandshakeMessage(transport transport.Transport, selfProtos []protocol.ID, tempChan bool) (*message.ProtoHandshakeMessage, errors.Error) {
	var tempChanUint8 uint8 = 0
	if tempChan == true {
		tempChanUint8 = 1
	}
	var toSend = message.NewProtoHandshakeMessage(selfProtos, p.config.ListenAddr, tempChanUint8)
	// log.Infof("Sending proto exchange message %+v", toSend)
	transport.SendMessage(protoMsgSerializer.Serialize(toSend))
	msgChan := transport.MessageChan()
	// log.Infof("Waiting for proto exchange message")
	select {
	case msgBytes, ok := <-msgChan:
		if !ok || len(msgBytes) == 0 {
			return nil, errors.NonFatalError(500, "Handshake failed", ProtoManagerCaller)
		}
		received := protoMsgSerializer.Deserialize(msgBytes).(message.ProtoHandshakeMessage)
		// log.Infof("Received proto exchange message: %+v", received)
		return &received, nil
	case <-time.After(p.config.HandshakeTimeout):
		panic("Handshake timed out")
	}

}

func waitDial(dialerProto protocol.ID, toDial peer.Peer, waitChan chan interface{}) {
	select {
	case <-waitChan:
		proto, _ := p.protocols.Load(dialerProto)
		_, connUp := p.activeTransports.Load(toDial.ToString())
		if !connUp {
			proto.(protocolValueType).DialFailed(toDial)
		} else {
			p.channelSubscribersMutex.Lock()
			subs := p.channelSubscribers[toDial.ToString()]
			subs[dialerProto] = true
			p.channelSubscribers[toDial.ToString()] = subs
			p.channelSubscribersMutex.Unlock()
			proto.(protocolValueType).DialSuccess(dialerProto, toDial)
		}
	case <-time.After(p.config.DialTimeout):
		proto, _ := p.protocols.Load(dialerProto)
		proto.(protocolValueType).DialFailed(toDial)
	}
}

func sendHandshakeMessage(transport transport.Transport, selfProtos []protocol.ID, tempChan bool) {
	var tempChanUint8 uint8 = 0
	if tempChan == true {
		tempChanUint8 = 1
	}
	var toSend = message.NewProtoHandshakeMessage(selfProtos, p.config.ListenAddr, tempChanUint8)
	// log.Infof("Sending proto exchange message %+v", toSend)
	transport.SendMessage(protoMsgSerializer.Serialize(toSend))
}

func Start() {

	p.protocols.Range(func(_, proto interface{}) bool {
		proto.(protocolValueType).Init()
		return true
	})

	go handleTransportListener()

	p.protocols.Range(func(_, proto interface{}) bool {
		go proto.(protocolValueType).Start()
		return true
	})

	logTicker := time.NewTicker(time.Second * 3)
	for {
		select {
		case <-logTicker.C:
			/*
				log.Info("------------- Protocol Manager state -------------")
				var toLog string
				toLog = "Active connections : "
				p.activeTransports.Range(func(peer, conn interface{}) bool {
					toLog += fmt.Sprintf("%s, ", peer.(string))
					return true
				})
				log.Info(toLog)
			*/
		}
	}
}
