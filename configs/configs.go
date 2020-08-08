package configs

import (
	"encoding/json"
	"io/ioutil"
	"time"
)

type ProtocolManagerConfig struct {
	HandshakeTimeout      time.Duration
	HeartbeatTickDuration time.Duration
	ConnectionReadTimeout time.Duration
	DialTimeout           time.Duration
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
