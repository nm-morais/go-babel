package utils

import "encoding/json"

func SerializeToJson(toMarshal interface{}) []byte {
	bytes, err := json.Marshal(toMarshal)
	if err != nil {
		panic(err)
	}
	return bytes
}

func DeserializeFromJson(bytes []byte, target interface{}) {
	err := json.Unmarshal(bytes, target)
	if err != nil {
		panic(err)
	}
}
