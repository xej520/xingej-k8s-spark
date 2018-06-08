package spark

import (
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	podfunc "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/resources/spark/pod"
	"time"
	status2 "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/service/status"
)

// 线程---创建单节点
func CreateNodeForCluster(rpc *RealPodControl, cluster *api.SparkCluster, status *api.ClusterStatus, serv *api.Server) {
	//每次间隔一秒钟，最多尝试5次
	for i := 0; i < 5; i++ {
		if doCreateNodeForCluster(rpc, cluster, status, serv) {
			//	成功后，需要给状态管道 发送消息
			updateStatus := status2.NewUpdateStatus(
				cluster, status, serv.Name, "", true)

			rpc.SendMsg2StatusChan(updateStatus)

			//结束循环
			return
		}

		// 失败后，间隔1秒钟执行一次
		time.Sleep(time.Second)
	}

	// 说明，单节点操作失败，已超过最大次数
	// 直接将节点状态改为failed
	updateStatus := status2.NewUpdateStatus(cluster, status, serv.Name, v1.PodFailed, false)

	rpc.SendMsg2StatusChan(updateStatus)

}

func doCreateNodeForCluster(rpc *RealPodControl, cluster *api.SparkCluster, status *api.ClusterStatus, serv *api.Server) bool {

	//创建单节点configMap
	if _, err := rpc.ConfigMapControl.GetForOne(cluster, serv); err != nil {
		if err = rpc.ConfigMapControl.CreateForOne(cluster, status, serv); err != nil {
			if !errors.IsAlreadyExists(err) {
				podfunc.AddConditions(status, api.ClusterConditionReady,
					v1.ConditionFalse, err.Error(),
					fmt.Sprintf("Create spark node configmap %q error", serv.Name))
				log.WarnErrorf(err, "create spark node configmap %q error", serv.Name)
				return false
			}
		}
	}

	//创建单节点服务
	if serv.Status == api.ServerWaiting {

		// 只有master角色，需要创建service
		if serv.Role == api.SparkRoleMaster {
			//创建单节点svc
			if svc, err := rpc.ServiceLister.Services(cluster.Namespace).Get(serv.Name); err != nil {
				if errors.IsNotFound(err) {
					if svc, err = podfunc.CreateService(cluster, status, serv.Name); err != nil {
						if !errors.IsAlreadyExists(err) {
							podfunc.AddConditions(status, api.ClusterConditionReady,
								v1.ConditionFalse, err.Error(),
								fmt.Sprintf("Create spark node service %q error", serv.Name))
							log.WarnErrorf(err, "create spark node service %q error", serv.Name)
							return false
						}

					}
				} else {
					log.WarnErrorf(err, "get spark node service %q error", serv.Name)
					return false
				}
			} else {
				if server, ok := status.ServerNodes[serv.Name]; ok {
					if server.Nodeport == 0 {
						for _, Ports := range svc.Spec.Ports {
							if Ports.NodePort != 0 {
								status.ServerNodes[serv.Name].Nodeport = Ports.NodePort
							}
						}
					}
				}
			}
		}

		//创建单节点pod
		if pod, err := rpc.PodLister.Pods(cluster.Namespace).Get(serv.Name); err != nil {
			if _, err := podfunc.CreatePod(cluster, status, serv.Name); err != nil {
				if !errors.IsAlreadyExists(err) {
					podfunc.AddConditions(status, api.ClusterConditionReady,
						v1.ConditionFalse, err.Error(),
						fmt.Sprintf("Create spark node pod %q error", serv.Name))
					log.WarnErrorf(err, "Create spark node pod %q error", serv.Name)
					return false
				}
			}
			log.Infof("Install spark server node %q succeeded", serv.Name)
		} else {
			serv.Status = pod.Status.Phase
		}
	}

	return true
}


