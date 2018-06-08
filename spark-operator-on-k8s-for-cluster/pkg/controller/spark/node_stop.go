package spark

import (

	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/api/core/v1"
	podfunc "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/resources/spark/pod"
	"time"
	status2 "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/service/status"
)

// 线程---停止单节点
func StopNodeForCluster(rpc *RealPodControl, cluster *api.SparkCluster, status *api.ClusterStatus, serv *api.Server) {
	//每次间隔一秒钟，最多尝试5次
	for i := 0; i < 5; i++ {
		if doStopNodeForCluster(rpc, cluster, status, serv) {
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

func doStopNodeForCluster(rpc *RealPodControl, cluster *api.SparkCluster, status *api.ClusterStatus, serv *api.Server) bool {
	//停止单节点服务
	if serv.Status == api.ServerWaiting {
		//删除单节点pod

		if err := podfunc.Stoppod(cluster, status, serv.Name); err != nil {
			log.WarnErrorf(err, "Stop spark node pod %q error ", serv.Name)
			return false
		}
		log.Infof("Stop spark node pod %q succeeded", serv.Name)
	}

	return true
}
