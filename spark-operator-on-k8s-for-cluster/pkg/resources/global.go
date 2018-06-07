package resources

import "fmt"

// 获取configmap的名称
func GetConfigMapName(name, role, id string)  string {
	return  fmt.Sprintf("spark-config-%s-%s-%v", name, role, id)
}

