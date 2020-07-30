package configs

import (
	"github.com/DeMMon/go-babel/pkg/utils"
	"io/ioutil"
)

type ProtocolManagerConfig struct {
	ListenPort int `json:"listen_port"`
}

func ReadConfigFile(filePath string) ProtocolManagerConfig {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	config := ProtocolManagerConfig{}
	utils.DeserializeFromJson(data, &config)
	return config
}
