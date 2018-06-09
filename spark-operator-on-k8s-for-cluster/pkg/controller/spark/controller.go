package spark

import (
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	"sync"
	"k8s.io/client-go/kubernetes"
	clientset "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/clientset/versioned"
	listers "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/listers/spark/v1beta1"
	"k8s.io/client-go/tools/cache"
	corelisters "k8s.io/client-go/listers/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/syncqueue"
	"k8s.io/client-go/tools/record"
	"time"
	kubeinformers "k8s.io/client-go/informers"
	informers "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/informers/externalversions"
	"github.com/prometheus/common/log"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/api/core/v1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/constants"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/k8sutil"
	"fmt"
	"k8s.io/apimachinery/pkg/api/errors"
	"reflect"
	status2 "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/service/status"
)

const (
	MessageResourceExists = "Resource %q already exists and is not managed by SparkCluster"
)

var controllerKind = api.SchemeGroupVersion.WithKind("SparkCluster")

type Controller struct {
	// controller 添加锁
	mu sync.Mutex

	kubeclientset kubernetes.Interface

	sparkClientset    clientset.Interface
	sparkLister       listers.SparkClusterLister
	sparkListerSynced cache.InformerSynced

	podLister       corelisters.PodLister
	podListerSynced cache.InformerSynced

	serviceLister       corelisters.ServiceLister
	serviceListerSynced cache.InformerSynced

	clusterUpdater   clusterUpdateInterface
	configMapControl ConfigMapControlInterface
	PodControl       podControlInterface

	workqueue *syncqueue.SyncQueue

	recorder record.EventRecorder

	statusCache map[string]*api.ClusterStatus

	// 缓存的异常pod信息，名称，以及异常时间
	abnormalPod map[string]*timeoutForPod
}

type timeoutForPod struct {
	startTime time.Time
	endTime   time.Time
}

func NewController(
	kubeclientset kubernetes.Interface,
	sparkClientset clientset.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory,
	sparkInformerFactory informers.SharedInformerFactory) *Controller {

	sparkInformer := sparkInformerFactory.Spark().V1beta1().SparkClusters()
	PodInformer := kubeInformerFactory.Core().V1().Pods()
	configmapInformer := kubeInformerFactory.Core().V1().ConfigMaps()
	servicesInformer := kubeInformerFactory.Core().V1().Services()

	log.Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{
		Interface: kubeclientset.CoreV1().Events(""),
	})

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{
		Component: "spark-cluster-controller",
	})

	controller := &Controller{
		kubeclientset:  kubeclientset,
		sparkClientset: sparkClientset,

		sparkLister:       sparkInformer.Lister(),
		sparkListerSynced: sparkInformer.Informer().HasSynced,

		podLister:       PodInformer.Lister(),
		podListerSynced: PodInformer.Informer().HasSynced,

		serviceLister:       servicesInformer.Lister(),
		serviceListerSynced: servicesInformer.Informer().HasSynced,

		recorder: recorder,
	}

	controller.abnormalPod = make(map[string]*timeoutForPod)

	controller.configMapControl = NewRealConfigMapControl(kubeclientset, configmapInformer)
	controller.clusterUpdater = NewClusterUpdater(sparkClientset, sparkInformer.Lister())
	controller.PodControl = NewRealPodControl(kubeclientset, sparkClientset, PodInformer.Lister(), servicesInformer.Lister(), controller.clusterUpdater, controller.configMapControl)

	controller.workqueue = syncqueue.NewPassthroughSyncQueue(&api.SparkCluster{}, controller.syncSparkCluster)

	controller.workqueue.SetMaxRetries(constants.MaxRetries)

	controller.statusCache = make(map[string]*api.ClusterStatus)

	log.Info("Setting up event handlers")

	sparkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.addSpark,
		UpdateFunc: controller.updateSpark,
		DeleteFunc: controller.deleteSpark,
	})

	PodInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: controller.updatePod,
	})

	return controller

}

func (c *Controller) addSpark(obj interface{}) {
	clus := obj.(*api.SparkCluster)
	log.Infof("Adding sparkcluster %s", clus.GetKey())

	c.workqueue.Enqueue(clus)
}

func (c *Controller) updateSpark(old, new interface{}) {
	oldD, newD := old.(*api.SparkCluster), new.(*api.SparkCluster)
	if oldD.ResourceVersion == newD.ResourceVersion {
		return
	}
	if newD.DeletionTimestamp != nil {
		return
	}
	// 定义是否有操作，只要操作不一致且操作标志位不能为空，则增加到队列中
	if !reflect.DeepEqual(oldD.Spec.SparkOperator, newD.Spec.SparkOperator) && newD.Spec.SparkOperator.Operator != "" {
		log.Infof("Updating sparkcluster %s", oldD.GetKey())

		c.workqueue.EnqueueAfter(newD, time.Second)

		return
	}
}

func (c *Controller) deleteSpark(obj interface{}) {
	//清除缓存中的信息
	mc, ok := obj.(*api.SparkCluster)
	if !ok {
		return
	}

	if _, ok := c.statusCache[mc.GetKey()]; ok {
		delete(c.statusCache, mc.GetKey())
		log.Infof("cluster %q has been deleted", mc.GetKey())
	}
}

func (c *Controller) updatePod(old, cur interface{}) {
	curPod := cur.(*v1.Pod)
	oldPod := old.(*v1.Pod)

	if curPod.ResourceVersion == oldPod.ResourceVersion {
		// Periodic resync will send update events for all known pods.
		// Two different versions of the same pod will always have different RVs.
		return
	}
	if curPod.DeletionTimestamp != nil {
		return
	}

	//获取节点对应集群信息
	controllerRef := metav1.GetControllerOf(curPod)
	if controllerRef == nil {
		// No controller should care about orphans being deleted.
		return
	}

	sparkC := c.resolveControllerRef(curPod.Namespace, controllerRef)
	if sparkC == nil {
		return
	}

	// 内存里获取最新状态信息
	status := c.getsparkClusterStatus(sparkC)
	// 从内存里获取当前的server实例
	if server, ok := status.ServerNodes[curPod.Name]; ok {
		if server.Status == "Running" && curPod.Status.Phase != "Running" {
			handleForExceptionPod(c, curPod, sparkC, status)
		} else if server.Status == "Failed" && curPod.Status.Phase == "Running" {
			//开始更新节点状态为running
			updateStatus := &status2.Updatestatus{
				Cluster:    sparkC,
				Status:     status,
				NodeName:   curPod.Name,
				NodeStatus: api.ServerRunning,
				Flag:       false,
			}
			c.PodControl.SendMsg2StatusChan(updateStatus)
			return
		} else if server.Status == "Failed" && curPod.Status.Phase != "Running" {
			handleForExceptionPod(c, curPod, sparkC, status)
		}
	} else {
		log.Errorf("spark cluster %q has no node %q ", sparkC.Name, curPod.Name)
	}
}

func (c *Controller) syncSparkCluster(obj interface{}) error {

	mc, ok := obj.(*api.SparkCluster)
	if !ok {
		return fmt.Errorf("expect sparkcluster, got %v", obj)
	}

	key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(mc)

	startTime := time.Now()
	log.Infof("Started syncing sparkcluster %q (%v)", key, startTime)
	defer func() {
		log.Infof("Finished syncing sparkcluster %q (%v)", key, time.Since(startTime))
	}()

	// Get the spark resource with this namespace/name
	clus, err := c.sparkLister.SparkClusters(mc.Namespace).Get(mc.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			if _, ok := c.statusCache[mc.GetKey()]; ok {
				delete(c.statusCache, mc.GetKey())
			}
			log.Infof("sparkcluster %v has been deleted", key)
			return nil
		}
		return err
	}

	// fresh spark
	if mc.UID != clus.UID {
		//  original spark is gone
		return nil
	}
	// 如果没有删除则开始处理
	if clus.DeletionTimestamp == nil {
		clusD := mc.DeepCopy()
		// 如果出现错误,会重试5次
		if err := c.SyncSpark(clusD); err != nil {
			c.recorder.Eventf(clusD, v1.EventTypeWarning, err.Error(),
				fmt.Sprintf("Process sparkcluster %v by error", key))

			log.Warnf("Process sparkcluster %v error, %v", key, err.Error())
			return err
		}
	} else {

	}

	return nil
}

func (c *Controller) SyncSpark(cluster *api.SparkCluster) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cluster.DeletionTimestamp != nil {
		return nil
	}

	if cluster.Spec.SparkOperator.Operator == api.ClusterOperatorPhaseCreate {
		cluster.SetDefaults()
	}

	status := c.getsparkClusterStatus(cluster)

	switch cluster.Spec.SparkOperator.Operator {
	case api.ClusterOperatorPhaseCreate:
		err = c.PodControl.CreateForCluster(cluster, status)
	case api.ClusterOperatorPhaseStop:
		err = c.PodControl.StopForCluster(cluster, status)
	case api.ClusterOperatorPhaseStart:
		err = c.PodControl.StartForCluster(cluster, status)
	case api.ClusterOperatorPhaseAddNode:
		err = c.PodControl.AddNodeForCluster(cluster, status)
	case api.ClusterOperatorPhaseChangeConfig:
		err = c.PodControl.ChangeConfigForCluster(cluster, status)
	case api.ClusterOperatorPhaseChangeResource:
		err = c.PodControl.ChangeResourceForCluster(cluster, status)
	case api.NodeOperatorPhaseDelete:
		err = c.PodControl.DeleteForNode(cluster, status)
	case api.NodeOperatorPhaseStart:
		err = c.PodControl.StartForNode(cluster, status)
	case api.NodeOperatorPhaseStop:
		err = c.PodControl.StopForNode(cluster, status)
	}

	return err

}

func (c *Controller) getsparkClusterStatus(cluster *api.SparkCluster) *api.ClusterStatus {
	status, ok := c.statusCache[cluster.GetKey()]
	if !ok {
		status = &api.ClusterStatus{}
		status.ClusterPhase = cluster.Status.ClusterPhase
		status.ResourceUpdateNeedRestart = cluster.Status.ResourceUpdateNeedRestart
		status.ParameterUpdateNeedRestart = cluster.Status.ParameterUpdateNeedRestart
		//status.CurrentConfig = cluster.Status.CurrentConfig

		for _, server := range cluster.Status.ServerNodes {
			server.Operator = cluster.Spec.SparkOperator.Operator
			//config.Changecnf(cluster, server)
		}

		status.ServerNodes = cluster.Status.ServerNodes
		status.WaitSparkComponentAvailableTimeout = cluster.Status.WaitSparkComponentAvailableTimeout
		status.Reason = cluster.Status.Reason
		status.Conditions = cluster.Status.Conditions

		c.statusCache[cluster.GetKey()] = status
	}

	return status
}


func (c *Controller) ensureResource() error {
	// init SparkCluster CRD
	if err := k8sutil.CreateCRD(spark.CrdKind); err != nil {
		return err
	}

	log.Infof("Create CustomResourceDefinition %s successfully",spark.CrdKind)
	return nil
}

func (c *Controller) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *api.SparkCluster {
	// We can't look up by UID, so look up by Name and then verify UID.
	// Don't even try to look up by Name if it's the wrong Kind.
	if controllerRef.Kind != controllerKind.Kind {
		return nil
	}
	sparkClus, err := c.sparkLister.SparkClusters(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if sparkClus.UID != controllerRef.UID {
		// The controller we found with this Name is not the same one that the
		// ControllerRef points to.
		return nil
	}
	return sparkClus
}

//处理异常pod的逻辑
func handleForExceptionPod(c *Controller, curPod *v1.Pod, sparkC *api.SparkCluster, status *api.ClusterStatus) {
	if tmpPod := c.abnormalPod[curPod.Name]; tmpPod != nil {
		c.abnormalPod[curPod.Name] = &timeoutForPod{
			startTime: time.Now(),
			endTime:   time.Now(),
		}
	} else {
		//计算当前异常pod的超时时间是否超过最大值
		timeout := c.abnormalPod[curPod.Name].endTime.Second() - c.abnormalPod[curPod.Name].startTime.Second()

		// 暂定超时时间是10分钟（pod的状态，在10分钟内一直没变）
		if timeout > 1*60*10{
			//开始更新节点状态，集群状态
			updateStatus := &status2.Updatestatus{
				Cluster:    sparkC,
				Status:     status,
				NodeName:   curPod.Name,
				NodeStatus: api.ServerFailed,
				Flag:       false,
			}
			c.PodControl.SendMsg2StatusChan(updateStatus)

			// 删除异常pod
			k8sutil.DeletePods(sparkC.Namespace, curPod.Name)

			return
		}

		// 更新endTime时间
		c.abnormalPod[curPod.Name].endTime = time.Now()

	}
}
