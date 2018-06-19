package main

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/constants"
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
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func main() {

	config, err := clientcmd.BuildConfigFromFlags(constants.ENDPOINT, constants.CONFIGPATH)

	if err != nil {
		return
	}

	kubeClient, e := clientset.NewForConfig(config)

	if err != nil {
		log.Warnf("--->获取K8s的clientset时失败!\n", err)
	}

	log.Info("--->获取到k8s的客户端kubeClient:\n", kubeClient)

	//crdClient, e := crdclientset.NewForConfig(config)

	sparkExtClient := util.MustNewSparkExtClient(constants.ENDPOINT, constants.CONFIGPATH)

	if e != nil {
		glog.Fatal(err)
	}

	log.Info("--->获取ext的客户端:\n", sparkExtClient)

	apiExtensionsClient, err := apiextensionsclient.NewForConfig(config)

	if err != nil {
		glog.Fatal(err)
	}

	log.Info("--->获取apiExtensionsClient的客户端:\t", apiExtensionsClient)

	crd.RegisterCRDForSparkApplication(constants.ENDPOINT, constants.CONFIGPATH)

	factory := crdinformers.NewSharedInformerFactory(sparkExtClient,
		// resyncPeriod. Every resyncPeriod, all resources in the cache will re-trigger events.
		// 每隔几秒钟，缓存中的所有资源，都重新激发一下事件
		time.Duration(15)*time.Second)

	//kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*15)

	//fmt.Println("----获取informerFactory的客户端:\t", kubeInformerFactory)

	controller := sparkapplication.NewController(sparkExtClient, kubeClient, apiExtensionsClient, factory, 6)

	stopCh := make(chan struct{})
	go factory.Start(stopCh)

	controller.Start(2, stopCh)

	//go kubeInformerFactory.Start(stopCh)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

}
