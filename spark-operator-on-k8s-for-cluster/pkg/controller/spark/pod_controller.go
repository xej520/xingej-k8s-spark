package spark

import (
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	"k8s.io/client-go/kubernetes"
	sparkop "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/clientset/versioned"
	corelisters "k8s.io/client-go/listers/core/v1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/service/status"
	"fmt"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"time"
	"strings"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/k8sutil"
)

type podControlInterface interface {
	CreateForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	StopForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	StartForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	AddNodeForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	ChangeResourceForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	ChangeConfigForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	StopForNode(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	StartForNode(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	DeleteForNode(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	ChangeClusterstate(cluster *api.SparkCluster, status *api.ClusterStatus) (error)
	ListenStatus(c *Controller, threadiness int) (error)
	SendMsg2StatusChan(*status.Updatestatus)
}

type RealPodControl struct {
	kubeclient kubernetes.Interface
	sparkClient sparkop.Interface

	PodLister corelisters.PodLister
	ServiceLister corelisters.ServiceLister

	ClusterUpdater clusterUpdateInterface
	ConfigMapControl ConfigMapControlInterface

	UpdateStatusChannel chan *status.Updatestatus
}

//func NewRealPodControl(client kubernetes.Interface, sparkclient sparkop.Interface,
//	podLister corelisters.PodLister, serviceLister corelisters.ServiceLister, clusterUpdater clusterUpdateInterface, configMapControl ConfigMapControlInterface) podControlInterface {
//
//		return &RealPodControl{kubeclient: client, sparkClient: sparkclient, PodLister: podLister,
//			ServiceLister: serviceLister, ClusterUpdater: clusterUpdater,
//			ConfigMapControl: configMapControl,
//			// 默认值是10个缓存
//			UpdateStatusChannel: make(chan *status.Updatestatus, 10),
//}


func (rpc *RealPodControl) CreateForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//1. 更新集群信息到etcd
	rpc.ClusterUpdater.UpdateClusterStatus(cluster, status)

	//2. 安装节点
	for _, serv := range status.ServerNodes {

		go handle(&createNodeThread{}, rpc, cluster, status, serv)
	}

	return nil

}

func (rpc *RealPodControl) StopForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//1. 修改集群和节点状态
	status.ClusterPhase = api.ClusterPhaseWaiting
	for _, serv := range status.ServerNodes {
		serv.Status = api.ServerWaiting
		serv.Operator = api.NodeOperatorPhaseStop
	}
	// 更新到etcd
	rpc.ClusterUpdater.UpdateClusterStatus(cluster, status)

	//2. 删除POD
	for _, serv := range status.ServerNodes {
		go handle(&stopNodeThread{}, rpc, cluster, status, serv)
	}

	return nil
}

func (rpc *RealPodControl) StartForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//1. 修改集群和节点状态
	status.ClusterPhase = api.ClusterPhaseWaiting
	for _, serv := range status.ServerNodes {
		serv.Status = api.ServerWaiting
		serv.Operator = api.NodeOperatorPhaseStart
	}

	rpc.ClusterUpdater.UpdateClusterStatus(cluster, status)

	//2. 启动POD
	for _, serv := range status.ServerNodes {
		go handle(&startNodeThread{}, rpc, cluster, status, serv)
	}

	return nil
}

func (rpc *RealPodControl) AddNodeForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//1. 初始化节点
	diff := cluster.Spec.Replicas - len(status.ServerNodes)

	if diff > 0 {
		if status.ServerNodes == nil {
			status.ServerNodes = make(map[string]*api.Server)
		}
		for i := 0; i < diff; {
			// 初始化名字
			serverID := utilrand.String(5)
			name := fmt.Sprintf("kafka-%s-%s-%s", cluster.Name, api.SparkRoleMaster, serverID)
			svcname := fmt.Sprintf("kafka-%s-%s-%s", cluster.Name, api.SparkRoleMaster, serverID)
			configmapname := fmt.Sprintf("kafka-config-%s-%s-%s", cluster.Name, api.SparkRoleMaster, serverID)
			volumeid := fmt.Sprintf("kafka-%s-%s-%s-%v", cluster.Namespace, cluster.Name, api.SparkRoleMaster, serverID)
			role := api.SparkRoleMaster
			yes := true
			for _, servernode := range status.ServerNodes {
				if serverID == servernode.ID {
					yes = false
					break
				}
			}
			if yes {
				status.ServerNodes[name] = &api.Server{ID: serverID, Name: name, Svcname: svcname, Configmapname: configmapname,
					VolumeID: volumeid, Role: role, Status: api.ServerWaiting, RestartAction: api.RestartActionNo,
				}
				i++
				//config.Changecnf(cluster, status.ServerNodes[name])

				//2. 更新到etcd
				rpc.ClusterUpdater.UpdateClusterStatus(cluster, status)

				//3. 安装节点
				go handle(&createNodeThread{}, rpc, cluster, status, status.ServerNodes[name])
			}

		}
	}

	return nil
}

func (rpc *RealPodControl) ChangeResourceForCluster(cluster *api.SparkCluster, clusterstatus *api.ClusterStatus) (error) {
	for _, serv := range clusterstatus.ServerNodes {
		serv.RestartAction = api.RestartActionNeed
	}

	updateStatus := status.NewUpdateStatus(cluster, clusterstatus, "", "cluster", false)

	rpc.SendMsg2StatusChan(updateStatus)

	return nil
}

func (rpc *RealPodControl) ChangeConfigForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {

	return nil
}

func (rpc *RealPodControl) StopForNode(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//0、查找出要停止的节点
	if server, ok := status.ServerNodes[cluster.Spec.SparkOperator.Node]; ok {
		log.Infof("stop node %s/%s", cluster.Namespace, server.Name)

		//1、修改集群状态waiting，修改节点状态waiting
		updateClusterAndServerStatus(server, status, api.NodeOperatorPhaseStop)

		//2、同步到etcd集群
		if err := rpc.ClusterUpdater.UpdateClusterStatus(cluster, status); err != nil {
			return fmt.Errorf("update kafka node %s%s to etcd failed", cluster.Namespace, server.Name)
		}

		//	3、对节点进行停止操作
		//go StopNodeForCluster(rpc, cluster, status, server)
		go handle(&stopNodeThread{}, rpc, cluster, status, server)

	} else {
		log.Errorf("kafka cluster %q has no node %q ", cluster.Name, cluster.Spec.SparkOperator.Node)
	}
	return nil
}

func (rpc *RealPodControl) StartForNode(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//0、查找出要操作的节点
	if server, ok := status.ServerNodes[cluster.Spec.SparkOperator.Node]; ok {
		log.Infof("start node %s/%s", cluster.Namespace, server.Name)

		//1、修改集群状态waiting，修改节点状态waiting
		updateClusterAndServerStatus(server, status, api.NodeOperatorPhaseStart)

		//2、同步到etcd集群
		if err := rpc.ClusterUpdater.UpdateClusterStatus(cluster, status); err != nil {
			return fmt.Errorf("update kafka node %s%s to etcd failed", cluster.Namespace, server.Name)
		}

		// 3、对节点进行启动操作
		//go StartNodeForCluster(rpc, cluster, status, server)
		go handle(&startNodeThread{}, rpc, cluster, status, server)
	} else {
		log.Errorf("kafka cluster %q has no node %q ", cluster.Name, cluster.Spec.SparkOperator.Node)
	}
	return nil
}

func (rpc *RealPodControl) DeleteForNode(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	//0、查找出要操作的节点
	if server, ok := status.ServerNodes[cluster.Spec.SparkOperator.Node]; ok {
		log.Infof("delete node %s/%s", cluster.Namespace, server.Name)

		//1、修改集群状态waiting，修改节点状态waiting
		updateClusterAndServerStatus(server, status, api.NodeOperatorPhaseDelete)

		// 2、同步到etcd集群
		if err := rpc.ClusterUpdater.UpdateClusterStatus(cluster, status); err != nil {
			return fmt.Errorf("update kafka node %s%s to etcd failed", cluster.Namespace, server.Name)
		}

		// 3、对节点进行删除操作
		//go DeleteNodeForCluster(rpc, cluster, status, server)
		go handle(&deleteNodeThread{}, rpc, cluster, status, server)
	} else {
		log.Errorf("kafka cluster %q has no node %q ", cluster.Name, cluster.Spec.SparkOperator.Node)
	}
	return nil
}

func (rpc *RealPodControl) ChangeClusterstate(cluster *api.SparkCluster, status *api.ClusterStatus) (error) {
	running := 0
	stopped := 0
	started := 0
	failed := 0
	waiting := false
	for _, servSpec := range status.ServerNodes {
		if servSpec.RestartAction == api.RestartActionStarted || servSpec.RestartAction == api.RestartActionNo {
			started = started + 1
		}
		if servSpec.Status == api.ServerWaiting {
			waiting = true
			break
		} else if servSpec.Status == api.ServerFailed {
			failed = failed + 1
			continue
		} else if servSpec.Status == api.ServerRunning {
			running = running + 1
			continue
		} else if servSpec.Status == api.ServerStopped {
			stopped = stopped + 1
			continue
		}
	}
	if stopped == len(status.ServerNodes) {
		status.ClusterPhase = api.ClusterPhaseStopped
	} else if running == len(status.ServerNodes) {
		status.ClusterPhase = api.ClusterPhaseRunning
	} else if failed == len(status.ServerNodes) {
		status.ClusterPhase = api.ClusterPhaseFailed
	} else {
		if !waiting {
			status.ClusterPhase = api.ClusterPhaseWarning
		}
	}
	if started == len(status.ServerNodes) {
		status.ParameterUpdateNeedRestart = false
		status.ResourceUpdateNeedRestart = false
		for _, servSpec := range status.ServerNodes {
			servSpec.RestartAction = api.RestartActionNo
		}
	}
	return nil
}

func (rpc *RealPodControl) ListenStatus(c *Controller, threadiness int) (error) {
	for {
		// 不断的从管道中取值
		updateStatus := <-rpc.UpdateStatusChannel

		if updateStatus.Flag {
			//说明 需要监听pod的状态， 获取节点状态
			podStatus := rpc.getPodStatus(updateStatus.Cluster.Namespace, updateStatus.NodeName, updateStatus.Cluster.Spec.SparkOperator.Operator)

			if server, ok := updateStatus.Status.ServerNodes[updateStatus.NodeName]; ok {

				if podStatus == api.ServerDeleted {
					// 删除操作，从缓存里删除
					delete(updateStatus.Status.ServerNodes, updateStatus.NodeName)
				}

				if podStatus == api.ServerStopped || podStatus == api.ServerFailed || podStatus == api.ServerRunning {
					server.Status = podStatus
				}

				if podStatus == api.ServerRunning && server.RestartAction == api.RestartActionNeed {
					server.RestartAction = api.RestartActionStarted
				}

				// unKonwn说明，在规定时间内，获取pod状态失败
				if podStatus == api.ServerUnknown {
					// 删除有问题的Pod
					server.Status = api.ServerFailed
					k8sutil.DeletePods(updateStatus.Cluster.Namespace, updateStatus.NodeName)
				}

				// 更新内存集群状态
				rpc.ChangeClusterstate(updateStatus.Cluster, updateStatus.Status)

				//将状态更新到etcd集群
				if len(updateStatus.Status.ServerNodes) == 0 {
					if err := rpc.sparkClient.SparkV1beta1().SparkClusters(updateStatus.Cluster.Namespace).Delete(updateStatus.Cluster.Name, &metav1.DeleteOptions{}); err != nil {
						log.WarnErrorf(err, "delete kafka cluster '%s' error", updateStatus.Cluster.GetKey())
					}
				} else {
					c.clusterUpdater.UpdateClusterStatus(updateStatus.Cluster, updateStatus.Status)
				}

			} else {
				log.Errorf("kafka cluster %q has no node %q ", updateStatus.Cluster.Name, updateStatus.NodeName)
			}

		} else {
			if updateStatus.NodeStatus == "cluster" {
				//修改集群状态
				rpc.ChangeClusterstate(updateStatus.Cluster, updateStatus.Status)

				//更新到etcd集群
				c.clusterUpdater.UpdateClusterStatus(updateStatus.Cluster, updateStatus.Status)
			} else {
				if server, ok := updateStatus.Status.ServerNodes[updateStatus.NodeName]; ok {
					// 直接将节点状态该为failed
					//if updateStatus.NodeStatus == "Failed" {
					//	server.Status = v1.PodFailed
					//}
					//
					//if updateStatus.NodeStatus == "Running" {
					//	server.Status = v1.PodRunning
					//}

					server.Status =   v1.PodPhase(updateStatus.NodeStatus )

					//修改集群状态
					rpc.ChangeClusterstate(updateStatus.Cluster, updateStatus.Status)

					//更新到etcd集群
					c.clusterUpdater.UpdateClusterStatus(updateStatus.Cluster, updateStatus.Status)
				} else {
					log.Errorf("kafka cluster %q has no node %q ", updateStatus.Cluster.Name, updateStatus.NodeName)
				}
			}

		}

	}
}

func (rpc *RealPodControl) SendMsg2StatusChan(statusChan *status.Updatestatus) {
	rpc.UpdateStatusChannel <- statusChan
}

//  针对节点操作，更新集群状态，节点状态为waiting
func updateClusterAndServerStatus(servSpec *api.Server, status *api.ClusterStatus, nodeOperator api.OperatorPhase) {

	servSpec.Status = api.ServerWaiting    //更新节点状态为waiting
	status.ClusterPhase = api.ClusterPhaseWaiting //集群状态waiting

	servSpec.Operator = nodeOperator
}

func (rpc *RealPodControl) getPodStatus(ns, name string, operator api.OperatorPhase) v1.PodPhase  {
	var timeout = 1

	// 判断操作类型
	opt := ""
	if strings.Contains(string(operator), "Stop") {
		opt = "Stop"
	} else if strings.Contains(string(operator), "Delete") {
		opt = "Delete"
	} else {
		opt = string(operator)
	}

	for {
		// 查询Pod的状态
		pod, err := rpc.PodLister.Pods(ns).Get(name)

		switch opt {
		case "Stop": // 当前操作是 停止操作
			if errors.IsNotFound(err) {
				return api.ServerStopped
			}
		case "Delete": // 当前操作是 删除操作(节点删除，集群删除)
			if errors.IsNotFound(err) {
				return api.ServerDeleted
			}
		default:
			// 针对 启动操作
			if err == nil { //说明查询到 对应的Pod的信息
				fmt.Println("----当前pod的状态是:\t", pod.Status.Phase)
				condflag := true
				for _, cond := range pod.Status.Conditions {
					if cond.Status != v1.ConditionTrue {
						condflag = false
					}
				}
				if pod.Status.Phase == v1.PodRunning && condflag {
					return pod.Status.Phase
				} else if pod.Status.Phase == v1.PodFailed {
					return pod.Status.Phase
				}
			}
		}

		// 超时机制
		if timeout > 120*5 {
			fmt.Println("-----获取pod状态阶段 超时结束-----")
			break //结束循环
		}

		timeout++

		// 每隔半秒 查询一次pod状态
		time.Sleep(time.Millisecond * 500)
	}

	//retry.Retry(60, time.Millisecond*500, func() (bool, error) {
	//})

	return api.ServerUnknown
}


















