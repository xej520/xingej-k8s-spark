package crd

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/crd/sparkapplication"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/util"
)

// 注册SparkApplication类型的CRD
func RegisterCRDForSparkApplication(endpoint, configPath string) {
	// 1、创建SparkApplication对应的CRD对象
	sparkAppCrdObject := sparkapplication.GetCRDObject()

	// 2、创建extension 客户端
	apiExtensionClient := util.KubeExtensionClient(endpoint, configPath)

	// 3、开始注册到k8s
	util.RegisterCRD(apiExtensionClient, sparkAppCrdObject)
}
