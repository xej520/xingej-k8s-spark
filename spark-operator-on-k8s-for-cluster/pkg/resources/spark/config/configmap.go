package config

import (
	"k8s.io/api/core/v1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/resources"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark"
)

type Config struct {
	ConfigMap *v1.ConfigMap
}

func NewConfigMap(clus *v1beta1.SparkCluster, server *v1beta1.Server) *v1.ConfigMap {
	//	初始化配置
	//data := map[string]string{}
	//log.Infof("newSparkcnf:")
	//data[]

	configMap := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "V1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      resources.GetConfigMapName(clus.Name, string(server.Role), server.ID),
			Namespace: clus.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(clus,
					schema.GroupVersionKind{
						Group:   v1beta1.SchemeGroupVersion.Group,
						Version: v1beta1.SchemeGroupVersion.Version,
						Kind:    spark.CrdKind,
					},
				),
			},
		},
	}

	return configMap
}
