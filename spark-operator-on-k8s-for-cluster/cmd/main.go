package main

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/signals"

	"fmt"
	"time"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/k8sutil"
	kubeinformers "k8s.io/client-go/informers"
	"github.com/CodisLabs/codis/pkg/utils/log"
	sparkInformers "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/informers/externalversions"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/controller/spark"
)

func main() {

	stopCh := signals.SetupSignalHandler()

	fmt.Println("-----hello")
	run(stopCh)
	log.Warn("spark-Operator unreachable，(｡•ˇ‸ˇ•｡)")
}

func run(stopCh <-chan struct{})  {
	k8sutil.MustInit()
	kubecli := k8sutil.MustNewKubeClient()

	sparkcli := k8sutil.MustNewSparkExtClient()

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubecli, time.Second*15)

	sparkInformerFactory := sparkInformers.NewSharedInformerFactory(sparkcli, time.Second*15)

	sparkClusterController := spark.NewController(kubecli,sparkcli, kubeInformerFactory,sparkInformerFactory)

	go kubeInformerFactory.Start(stopCh)

	go sparkInformerFactory.Start(stopCh)

	go sparkClusterController.PodControl.ListenStatus(sparkClusterController, 2)

	if err := sparkClusterController.Run(2, stopCh); err != nil {
		log.PanicError(err, "Error running controller")
	}

}
