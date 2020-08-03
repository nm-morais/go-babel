package configs

import (
	"encoding/json"
	"io/ioutil"
)

type ProtocolManagerConfig struct {
	ListenPort int16 `json:"listen_port"`
}

func ReadConfigFile(filePath string) ProtocolManagerConfig {
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
