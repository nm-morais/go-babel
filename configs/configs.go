package configs

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/nm-morais/go-babel/pkg/peer"
)

type ProtocolManagerConfig struct {
	LogFolder             string
	HandshakeTimeout      time.Duration
	HeartbeatTickDuration time.Duration
	ConnectionReadTimeout time.Duration
	DialTimeout           time.Duration
	Peer                  peer.Peer
}

func ReadConfigFromFile(filePath string) ProtocolManagerConfig {
	//TODO
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	config := ProtocolManagerConfig{}
	if err := json.Unmarshal(data, &config); err != nil {
		panic(err)
	}
	return config
}
