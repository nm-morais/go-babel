package configs

import (
	"encoding/json"
	"io/ioutil"
	"net"
)

type ProtocolManagerConfig struct {
	ListenAddr net.Addr
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