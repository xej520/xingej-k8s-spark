package sparkapplication

import (
	crdclientset "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/clientset/versioned"
	clientset "k8s.io/client-go/kubernetes"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/tools/cache"
	crdlisters "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/listers/spark/v1beta1"
	"k8s.io/client-go/tools/record"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdscheme "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/clientset/versioned/scheme"
	"k8s.io/client-go/kubernetes/scheme"
	"github.com/golang/glog"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	apiv1 "k8s.io/api/core/v1"
)

type Controller struct {
	crdClient        crdclientset.Interface
	kubeClient       clientset.Interface
	extensionsClient apiextensionsclient.Interface
	queue            workqueue.RateLimitingInterface
	cacheSynced      cache.InformerSynced
	lister           crdlisters.SparkApplicationLister
	recorder         record.EventRecorder
	runner           *sparkSubmitRunner
}

// NewController 与 newSparkApplicationController相当于java中的构造方法
// NewController 在 newSparkApplicationController 基础上，再一次封装，添加了event事件记录
// 这样，利用kubectl就可以查询到相应的信息了

func NewController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	informerFactory crdinformers.SharedInformerFactory,
	submissionRunnerWorkers int) *Controller {
	crdscheme.AddToScheme(scheme.Scheme)

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.V(2).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(apiv1.NamespaceAll),
	})

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "spark-operator"})

	return newSparkApplicationController(crdClient, kubeClient, extensionsClient, informerFactory, recorder, submissionRunnerWorkers)
}

func newSparkApplicationController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	informerFactory crdinformers.SharedInformerFactory,
	eventRecorder record.EventRecorder,
	submissionRunnerWorkers int) *Controller {
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "spark-application-controller")
	runner := newSparkSubmitRunner(submissionRunnerWorkers)

	controller := &Controller{
		crdClient:        crdClient,
		kubeClient:       kubeClient,
		extensionsClient: extensionsClient,
		recorder:         eventRecorder,
		queue:            queue,
		runner:           runner,
	}

	informer := informerFactory.Sparkoperator().V1alpha1().SparkApplications()

	//添加 监听事件
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})

	return nil
}

func (c *Controller) onAdd(obj interface{}) {

}

func (c *Controller) onUpdate(oldObj, newObj interface{}) {

}

func (c *Controller) onDelete(obj interface{}) {

}
