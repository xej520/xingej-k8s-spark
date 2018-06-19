package config

import (
	"k8s.io/api/core/v1"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/resources"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"strings"
)

type Config struct {
	ConfigMap *v1.ConfigMap
}

func NewForCluster(clus *v1beta1.SparkCluster, server *v1beta1.Server) *Config {
	config := &Config{}

	config.ConfigMap = NewConfigMap(clus, server)

	return config
}

func NewConfigMap(clus *v1beta1.SparkCluster, server *v1beta1.Server) *v1.ConfigMap {
	//	初始化配置
	data := map[string]string{}
	log.Infof("---------newSparkcnf:---------")
	data["spark-env.sh"] = MergeConfig(clus.Spec.Config.Sparkcnf, server.Svcname)

	configMap := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
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
		Data:data,
	}

	return configMap
}

func GetConfigMapName(clusterName string, role string, serverId string) string {
	return fmt.Sprintf("spark-config-%s-%s-%s", clusterName, role, serverId)
}

func MergeConfig(parameters map[string]string, svcName string) string  {
	var sparkEnvParams  []string

	for k, v := range parameters {
		sparkEnvParams = append(sparkEnvParams, fmt.Sprintf("export %s=%s\n", k, v))
	}

	if strings.Contains(svcName, "slave") {
		svcName = strings.Replace(svcName,"slave", "master", 1)
	}

   sparkEnvParams = append(sparkEnvParams, fmt.Sprintf("export %s=%s\n", "SPARK_MASTER_HOST", svcName))

	sparkEnv := strings.Join(sparkEnvParams, "")

	log.Info("-------------->:\n", sparkEnv)

	return sparkEnv
}
