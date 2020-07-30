package pkg

import (
	"github.com/DeMMon/go-babel/configs"
	. "github.com/DeMMon/go-babel/pkg/message"
	"github.com/DeMMon/go-babel/pkg/peer"
	. "github.com/DeMMon/go-babel/pkg/protocol"
	"github.com/DeMMon/go-babel/pkg/request"
	"github.com/DeMMon/go-babel/pkg/utils"
)

const caller = "ProtocolManager"
const configFilePath = "./configs/config.json"

type protocolManager struct {
	config    configs.ProtocolManagerConfig
	protocols map[string]Protocol
}

type IProtocolManager interface {
	Protocol(id ID) (Error, Protocol)
	SendMessage(message Message, peer peer.Peer, protocol Protocol) Error
	SendRequest(request request.Request, origin Protocol, destination Protocol) Error
}

var pm *protocolManager

func NewProtocolManager() IProtocolManager {
	if pm == nil {
		pm = &protocolManager{
			config:    configs.ReadConfigFile(configFilePath),
			protocols: make(map[string]Protocol),
		}
	}
	return pm
}

func (pm *protocolManager) RegisterProtocol(protocol Protocol) Error {
	_, exists := pm.protocols[protocol.ID()]
	if !exists {
		return utils.FatalError(409, "protocol already registered", caller)
	}
	wrapperProtocol := NewProtocolWrapper(protocol)
	pm.protocols[protocol.ID()] = wrapperProtocol
	return nil
}

func (pm *protocolManager) Protocol(id ID) (Error, Protocol) {
	return nil, nil //TODO
}

func (pm *protocolManager) SendMessage(message Message, peer peer.Peer, protocol Protocol) Error {
	return nil //TODO
}

func (pm *protocolManager) SendRequest(request request.Request, origin Protocol, destination Protocol) Error {
	return nil //TODO
}
