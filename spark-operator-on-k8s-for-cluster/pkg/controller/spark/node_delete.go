package spark

import (
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	"github.com/CodisLabs/codis/pkg/utils/log"
	podfunc "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/resources/spark/pod"
	"time"
	"k8s.io/api/core/v1"
	status2 "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/service/status"
)
// 线程---删除单节点
func DeleteNodeForCluster(rpc *RealPodControl, cluster *api.SparkCluster, status *api.ClusterStatus, serv *api.Server) {
	//每次间隔一秒钟，最多尝试5次
	for i := 0; i < 5; i++ {
		if doDeleteNodeForCluster(rpc, cluster, status, serv) {
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


func doDeleteNodeForCluster(rpc *RealPodControl, cluster *api.SparkCluster, status *api.ClusterStatus, serv *api.Server) bool {
	//删除单节点服务
	if serv.Status == api.ServerWaiting {
		// 调用k8s接口，删除server， pod，(在停止状态pod已经删除了，不需要删除pod)
		if err := podfunc.Uninstall(cluster, status, serv.Name); err != nil {
			log.WarnErrorf(err, "delete kafka node pod and svc %s%s failed", cluster.Namespace, serv.Name)
			return false
		}

		// 调用k8s接口，删除configMap
		if err := rpc.ConfigMapControl.DeleteForOne(cluster, serv); err != nil {
			log.WarnErrorf(err, "delete kafka node configmap %s%s failed", cluster.Namespace, serv.Name)
			return false
		}

		log.Infof("Stop kafka node pod %q succeeded", serv.Name)
	}

	return true
}
