package json

import "encoding/json"

func Struct2json(obj interface{}) interface{} {
	data, err := json.Marshal(obj)

	if  err != nil{
		return nil
	}

	return string(data)
}
