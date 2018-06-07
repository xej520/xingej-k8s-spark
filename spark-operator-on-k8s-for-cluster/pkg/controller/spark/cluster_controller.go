package spark

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	sparkOp "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/clientset/versioned"
	listers "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/listers/spark/v1beta1"
	"k8s.io/client-go/util/retry"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type clusterUpdateInterface interface {
	UpdateClusterStatus(cluster *v1beta1.SparkCluster, status *v1beta1.ClusterStatus, spec *v1beta1.ClusterSpec) error
}

type clusterUpdate struct {
	client sparkOp.Interface
	lister listers.SparkClusterLister
}

func NewClusterUpdater(client sparkOp.Interface, lister listers.SparkClusterLister) clusterUpdateInterface {
	return &clusterUpdate{
		client: client,
		lister: lister}
}

func (c *clusterUpdate) UpdateClusterStatus(cluster *v1beta1.SparkCluster, status *v1beta1.ClusterStatus, spec *v1beta1.ClusterSpec) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cluster.Status = *status
		cluster.Spec = *spec
		_, updateErr := c.client.SparkV1beta1().SparkClusters(cluster.Namespace).Update(cluster)
		if updateErr == nil {
			return nil
		}

		updated, err := c.lister.SparkClusters(cluster.Namespace).Get(cluster.Name)

		if err != nil {
			log.ErrorErrorf(err, "Error getting updated KafkaCluster %s/%s", cluster.Namespace, cluster.Name)
			return err
		}

		cluster = updated.DeepCopy()

		return updateErr
	})
}
