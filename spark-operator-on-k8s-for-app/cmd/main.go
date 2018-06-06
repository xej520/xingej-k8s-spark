package main

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/constants"
	"log"
	"fmt"
	"k8s.io/client-go/tools/clientcmd"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/controller/sparkapplication"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	crdinformers "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/informers/externalversions"
	"github.com/golang/glog"
	clientset "k8s.io/client-go/kubernetes"
	"time"
	"os"
	"os/signal"
	"syscall"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/crd"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/util"
)

func main() {

	config, err := clientcmd.BuildConfigFromFlags(constants.ENDPOINT, constants.CONFIGPATH)

	if err != nil {
		return
	}

	kubeClient, e := clientset.NewForConfig(config)

	if err != nil {
		log.Println("--->获取K8s的clientset时失败!\n", err)
	}

	fmt.Println("----获取到k8s的客户端:\t", kubeClient)

	//crdClient, e := crdclientset.NewForConfig(config)

	sparkExtClient := util.MustNewSparkExtClient(constants.ENDPOINT, constants.CONFIGPATH)

	if e != nil {
		glog.Fatal(err)
	}

	fmt.Println("----获取ext的客户端:\t", sparkExtClient)

	apiExtensionsClient, err := apiextensionsclient.NewForConfig(config)

	if err != nil {
		glog.Fatal(err)
	}

	fmt.Println("----获取apiExtensionsClient的客户端:\t", apiExtensionsClient)

	crd.RegisterCRDForSparkApplication(constants.ENDPOINT, constants.CONFIGPATH)


	fmt.Println("----注册CRD:\t")

	factory := crdinformers.NewSharedInformerFactory(sparkExtClient,
		// resyncPeriod. Every resyncPeriod, all resources in the cache will re-trigger events.
		// 每隔几秒钟，缓存中的所有资源，都重新激发一下事件
		time.Duration(15)*time.Second)

	fmt.Println("----注册factory:\t")

	//kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*15)

	//fmt.Println("----获取informerFactory的客户端:\t", kubeInformerFactory)

	fmt.Println("-------启动完成kubeInformerFactory-----")

	controller := sparkapplication.NewController(sparkExtClient, kubeClient, apiExtensionsClient, factory, 6)

	stopCh := make(chan struct{})
	go factory.Start(stopCh)

	controller.Start(2, stopCh)

	//go kubeInformerFactory.Start(stopCh)


	fmt.Println("-------1-----")
	signalCh := make(chan os.Signal, 1)
	fmt.Println("-------2-----")
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("-------3-----")
	<-signalCh
	fmt.Println("-------4-----")

}
