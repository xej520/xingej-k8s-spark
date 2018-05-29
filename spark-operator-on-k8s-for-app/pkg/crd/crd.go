package crd

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/crd/sparkapplication"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"fmt"
)

// 注册SparkApplication类型的CRD
func RegisterCRDForSparkApplication(endpoint, configPath string) {
	// 1、创建extension 客户端
	apiExtensionClient := util.KubeExtensionClient(endpoint, configPath)

	// 2、创建SparkApplication对应的CRD对象
	sparkAppCrdObject := sparkapplication.GetCRDObject()

	//3、校验是否已经注册过CRD
	if existing, err := apiExtensionClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(sparkAppCrdObject.Name, v1.GetOptions{});
		err == nil && existing != nil {
		// 说明已经注册过了
		// 就不再进行重复注册，当前处理逻辑：退出即可
		fmt.Printf("已经注册过 %s CRD了", sparkAppCrdObject.Name)
		return
	}

	// 4、开始注册到k8s
	util.RegisterCRD(apiExtensionClient, sparkAppCrdObject)
}
