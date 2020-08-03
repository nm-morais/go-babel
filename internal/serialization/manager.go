package serialization

import (
	"fmt"
	"github.com/nm-morais/go-babel/pkg/message"
	"sync"
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

func (m *Manager) GetDeserializer(id message.ID) message.Deserializer {
	deserializer, ok := m.deserializers.Load(id)
	if !ok {
		panic(fmt.Sprintf("No deserializer for messageID: %d", id))
	}
	return deserializer.(message.Deserializer)
}

func (m *Manager) GetSerializer(id message.ID) message.Serializer {
	serializer, ok := m.serializers.Load(id)
	if !ok {
		panic(fmt.Sprintf("No serializer for messageID: %d", id))
	}
	return serializer.(message.Serializer)
}

func (m *Manager) RegisterDeserializer(id message.ID, serializer message.Deserializer) {
	m.deserializers.Store(id, serializer)
}

func (m *Manager) RegisterSerializer(id message.ID, serializer message.Serializer) {
	m.deserializers.Store(id, serializer)
}

func (m *Manager) Deserialize(id message.ID, bytes []byte) message.Message {
	return m.GetDeserializer(id).Deserialize(bytes)
}

func (m *Manager) Serialize(message message.Message) []byte {
	return m.GetSerializer(message.Type()).Serialize(message)
}
