package serialization

import (
	"fmt"
	"github.com/nm-morais/DeMMon/go-babel/pkg/message"
	"github.com/nm-morais/DeMMon/go-babel/pkg/serialization"
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

func (m *Manager) GetDeserializer(id message.ID) serialization.Deserializer {
	deserializer, ok := m.deserializers.Load(id)
	if !ok {
		panic(fmt.Sprintf("No deserializer for messageID: %d", id))
	}
	return deserializer.(serialization.Deserializer)
}

func (m *Manager) GetSerializer(id message.ID) serialization.Serializer {
	serializer, ok := m.serializers.Load(id)
	if !ok {
		panic(fmt.Sprintf("No serializer for messageID: %d", id))
	}
	return serializer.(serialization.Serializer)
}

func (m *Manager) RegisterDeserializer(id message.ID, serializer serialization.Deserializer) {
	m.deserializers.Store(id, serializer)
}

func (m *Manager) RegisterSerializer(id message.ID, serializer serialization.Serializer) {
	m.deserializers.Store(id, serializer)
}

func (m *Manager) Deserialize(id message.ID, bytes []byte) message.Message {
	return m.GetDeserializer(id).Deserialize(bytes)
}

func (m *Manager) Serialize(message message.Message) []byte {
	return m.GetSerializer(message.Type()).Serialize(message)
}
