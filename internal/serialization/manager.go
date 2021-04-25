package serialization

import (
	"errors"
	"sync"

	"github.com/nm-morais/go-babel/pkg/message"
)

var (
	ErrDeserialization = errors.New("deserialization error")
	ErrNoSerializer    = errors.New("no serializer for message")
	ErrNoDeserializer  = errors.New("no deserializer for message")
)

type Manager struct {
	serializers   *sync.Map
	deserializers *sync.Map
}

func NewSerializationManager() *Manager {
	return &Manager{
		serializers:   &sync.Map{},
		deserializers: &sync.Map{},
	}
}

func (m *Manager) RegisterDeserializer(id message.ID, serializer message.Deserializer) {
	m.deserializers.Store(id, serializer)
}

func (m *Manager) RegisterSerializer(id message.ID, serializer message.Serializer) {
	m.serializers.Store(id, serializer)
}

func (m *Manager) Deserialize(id message.ID, bytes []byte) (message.Message, error) {
	serializer, ok := m.deserializers.Load(id)
	if !ok {
		return nil, ErrNoSerializer
	}
	msgBytes := serializer.(message.Deserializer).Deserialize(bytes)
	return msgBytes, nil
}

func (m *Manager) Serialize(msg message.Message) ([]byte, error) {
	serializerGeneric, ok := m.serializers.Load(msg.Type())
	if !ok {
		return nil, ErrNoDeserializer
	}
	msgBytes := serializerGeneric.(message.Serializer).Serialize(msg)
	return msgBytes, nil
}
