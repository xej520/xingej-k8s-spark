package sparkapplication

import (
	crdclientset "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/clientset/versioned"
	clientset "k8s.io/client-go/kubernetes"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/client-go/tools/cache"
	crdlisters "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/listers/spark/v1beta1"
	"k8s.io/client-go/tools/record"
	crdinformers "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/informers/externalversions"
	crdscheme "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/client/clientset/versioned/scheme"
	"k8s.io/client-go/kubernetes/scheme"
	"github.com/golang/glog"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	apiv1 "k8s.io/api/core/v1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/apis/spark/v1beta1"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/apimachinery/pkg/util/runtime"
)

var (
	keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc
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

	// 将sparkapplication 注册到scheme.Scheme里
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

	informer := informerFactory.Sparkoperator().V1beta1().SparkApplications()

	//添加 监听事件
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})

	controller.cacheSynced = informer.Informer().HasSynced
	controller.lister = informer.Lister()

	return controller
}

func (c *Controller) onAdd(obj interface{}) {
	application := obj.(*v1beta1.SparkApplication)
	log.Info("----onAdd----->:\t", application.Spec)
	c.enqueue(obj)
}

func (c *Controller) onUpdate(oldObj, newObj interface{}) {
	fmt.Println("----onUpdate----->:\t")
}

func (c *Controller) onDelete(obj interface{}) {
	fmt.Println("----onDelete----->:\t")
}

func (c *Controller) Start(workers int, stopCh <- chan struct{})  error {
	log.Info("Starting the Spark Application controller")

	if !cache.WaitForCacheSync(stopCh, c.cacheSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}

	fmt.Println("Starting thr workers of the SparkApplication controller")
	for i :=0; i < workers; i++ {
		log.Info("----workers------")
	}

	return nil
}

func (c *Controller) enqueue(obj interface{})  {
	key, err := keyFunc(obj)

	if err != nil {
		glog.Errorf("--->failed to get key for %v : %v", obj,  err)
		return
	}

	c.queue.AddRateLimited(key)

}

func (c *Controller) runWorker()  {
	
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}

	defer c.queue.Done(key)

	err := c.syncSparkApplication(key.(string))

	if err == nil {
		c.queue.Forget(key)
		return true
	}

	runtime.HandleError(fmt.Errorf("failed to sync SparkApplication %q: %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

func (c *Controller) syncSparkApplication(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)

	if err != nil {
		return fmt.Errorf("failed to get the namespace and name from key %s: %v", key, err)
	}

	application, err := c.getSparkApplication(namespace, name)

	if err != nil {
		return err
	}

	err = c.createSubmission(application)
	if err != nil {
		c.recorder.Eventf(
			application,
			apiv1.EventTypeWarning,
			"SparkApplicationSubmissionCreationFailed",
			"failed to create a submission for SparkApplication %s: %s",
			application.Name,
			err.Error()	)

		return err
	}

	return nil
}

func (c *Controller) createSubmission(app *v1beta1.SparkApplication) error {

	return nil
}

func (c *Controller) getSparkApplication(namespace, name string) (*v1beta1.SparkApplication, error) {
	return c.lister.SparkApplications(namespace).Get(name)
}












