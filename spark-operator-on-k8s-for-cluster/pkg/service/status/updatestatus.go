package status

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	"k8s.io/api/core/v1"
)

// 状态管道通信消息
type Updatestatus struct {
	Cluster    *v1beta1.SparkCluster
	Status     *v1beta1.ClusterStatus
	NodeName   string
	NodeStatus v1.PodPhase
	Flag       bool
}

func NewUpdateStatus(cluster *v1beta1.SparkCluster,
	status *v1beta1.ClusterStatus,
	nodeName string,
	nodeStatus v1.PodPhase,
	flag bool) *Updatestatus {

	return &Updatestatus{
		Cluster:    cluster,
		Status:     status,
		NodeName:   nodeName,
		NodeStatus: nodeStatus,
		Flag:       flag,
	}

}
