package spark

import (
	"k8s.io/client-go/kubernetes"
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/resources/spark/config"
	corev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"fmt"
)

type ConfigMapControlInterface interface {
	GetForOne(cluster *api.SparkCluster, server *api.Server) (conf *config.Config, err error)
	CreateForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (err error)
	CreateForOne(cluster *api.SparkCluster, status *api.ClusterStatus, server *api.Server) (err error)
	DeleteForCluster(cluster *api.SparkCluster) ( err error)
	DeleteForOne(cluster *api.SparkCluster, server *api.Server) (err error)
	IsControllerByCluster(cluster *api.SparkCluster, conf *config.Config) (bool, string)
	UpdateForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (err error)
	UpdateForOne(cluster *api.SparkCluster, server *api.Server) (err error)
}

type realConfigMapControl struct {
	kubeclient      kubernetes.Interface
	configMapLister corev1.ConfigMapInformer
}

func NewRealConfigMapControl(kubeclient kubernetes.Interface, configMapLister corev1.ConfigMapInformer) ConfigMapControlInterface {
	return &realConfigMapControl{
		kubeclient:      kubeclient,
		configMapLister: configMapLister}
}

func (rmc *realConfigMapControl) GetForOne(cluster *api.SparkCluster, server *api.Server) (conf *config.Config, err error) {
	conf = &config.Config{}

	var configmap *v1.ConfigMap

	configName := config.GetConfigMapName(cluster.Name, string(server.Role), server.ID)

	if configmap, err = rmc.kubeclient.CoreV1().ConfigMaps(cluster.Namespace).Get(configName, metav1.GetOptions{}); err != nil {
		return conf, err
	}

	conf.ConfigMap = configmap

	return conf, nil
}

func (rmc *realConfigMapControl) CreateForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (err error) {

	for _, server := range status.ServerNodes {
		if err = rmc.CreateForOne(cluster, status, server); err != nil {
			return err
		}
	}

	return nil
}

func (rmc *realConfigMapControl) CreateForOne(cluster *api.SparkCluster, status *api.ClusterStatus, server *api.Server) (err error) {
	conf := config.NewForCluster(cluster, server)

	if _, err := rmc.kubeclient.CoreV1().ConfigMaps(cluster.Namespace).Create(conf.ConfigMap); err != nil {
		return err
	}

	return nil
}

func (rmc *realConfigMapControl) DeleteForCluster(cluster *api.SparkCluster) (err error) {

	for _, server := range cluster.Status.ServerNodes {
		if err := rmc.DeleteForOne(cluster, server); err != nil {
			return err
		}
	}
	return nil
}

func (rmc *realConfigMapControl) DeleteForOne(cluster *api.SparkCluster, server *api.Server) (err error) {

	name := config.GetConfigMapName(cluster.Name, string(server.Role), server.ID)

	if err := rmc.kubeclient.CoreV1().ConfigMaps(cluster.Namespace).Delete(name, &metav1.DeleteOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (rmc *realConfigMapControl) IsControllerByCluster(cluster *api.SparkCluster, conf *config.Config) (bool, string) {

	// 检测configmap是否属于cluster
	if !metav1.IsControlledBy(conf.ConfigMap, cluster) {
		msg := fmt.Sprintf(MessageResourceExists, conf.ConfigMap.Name)
		return false, msg
	}
	return true, ""
}

func (rmc *realConfigMapControl) UpdateForCluster(cluster *api.SparkCluster, status *api.ClusterStatus) (err error) {

	for _, server := range status.ServerNodes {
		if err = rmc.UpdateForOne(cluster, server); err != nil {
			return err
		}
	}
	return nil

}

func (rmc *realConfigMapControl) UpdateForOne(cluster *api.SparkCluster, server *api.Server) (err error) {

	return nil
}
