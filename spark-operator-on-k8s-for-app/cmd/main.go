package main

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/crd"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/constants"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/util"
	"log"
	"fmt"
)

func main() {
	clientset, err := util.GetK8sClientByClientset(constants.ENDPOINT)

	if err != nil {
		log.Println("--->获取K8s的clientset时失败!\n", err)
	}

	fmt.Println("----获取到k8s的客户端:\t", clientset)

	crd.RegisterCRDForSparkApplication(constants.ENDPOINT, constants.CONFIGPATH)
}
