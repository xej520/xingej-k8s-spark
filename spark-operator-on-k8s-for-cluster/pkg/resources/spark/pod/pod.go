package pod

import (
	defaulterror "errors"
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark"
	"time"
	"k8s.io/api/core/v1"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/k8sutil"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/api/errors"
	"encoding/json"
)

var (
	defaultTrminationGracePeriodSeconds int64 = 5
)

const (
	nodeSelectHostname = "kubernetes.io/hostname"
)

// 安装节点
func Install(cluster *api.SparkCluster, status *api.ClusterStatus, nodename string) (err error) {
	// 创建service
	if _, err = CreateService(cluster, status, nodename); err != nil {
		AddConditions(status, api.ClusterConditionReady,
			v1.ConditionFalse, err.Error(),
			fmt.Sprintf("Create spark cluster %q service error", nodename))
		return err
	}
	// 创建pod
	if _, err = CreatePod(cluster, status, nodename); err != nil {
		AddConditions(status, api.ClusterConditionReady,
			v1.ConditionFalse, err.Error(),
			fmt.Sprintf("Create spark cluster %q pod  error", nodename))
		return err
	}
	return nil
}

//停止节点
func Stoppod(cluster *api.SparkCluster, status *api.ClusterStatus, nodename string) error {
	// 删除pod
	if err := k8sutil.DeletePods(cluster.Namespace, nodename); err != nil {
		if !errors.IsNotFound(err) {
			AddConditions(status, api.ClusterConditionReady,
				v1.ConditionFalse, err.Error(),
				fmt.Sprintf("Delete spark cluster %q server  error", nodename))
			return err
		} else {
			AddConditions(status, api.ClusterConditionReady,
				v1.ConditionTrue, "",
				fmt.Sprintf("Delete spark cluster %q service succeed", nodename))
		}
	}

	return nil
}

// stopped状态的节点删除
func DeleteServices(cluster *api.SparkCluster, status *api.ClusterStatus, nodename string) error {
	// 删除service
	if err := k8sutil.DeleteServices(cluster.Namespace, nodename); err != nil {
		if !errors.IsNotFound(err) {
			AddConditions(status, api.ClusterConditionReady,
				v1.ConditionFalse, err.Error(),
				fmt.Sprintf("Delete spark cluster %q service  error", nodename))
			return err
		}
	}
	return nil
}

// 卸载节点
func Uninstall(cluster *api.SparkCluster, status *api.ClusterStatus, nodename string) error {

	if server, ok := status.ServerNodes[nodename]; ok {

		if server.Role == api.SparkRoleMaster {
			// 删除service
			if err := k8sutil.DeleteServices(cluster.Namespace, nodename); err != nil {
				if !errors.IsNotFound(err) {
					AddConditions(status, api.ClusterConditionReady,
						v1.ConditionFalse, err.Error(),
						fmt.Sprintf("Delete spark cluster %q service  error", nodename))
					return err
				}
			}
		}

	}

	// 删除pod
	if err := k8sutil.DeletePods(cluster.Namespace, nodename); err != nil {
		if !errors.IsNotFound(err) {
			AddConditions(status, api.ClusterConditionReady,
				v1.ConditionFalse, err.Error(),
				fmt.Sprintf("Delete spark cluster %q server  error", nodename))
			return err
		}
	}

	return nil
}

// 创建service
func CreateService(cluster *api.SparkCluster, status *api.ClusterStatus, nodename string) (*v1.Service, error) {

	labels := GetLabels(cluster.Name, nodename)
	name := nodename

	var srv = &v1.Service{}

	if status.ServerNodes[name].Nodeport > 0 {
		srv = &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       cluster.Namespace,
				OwnerReferences: GetOwners(cluster),
			},
			Spec: v1.ServiceSpec{
				Type:     v1.ServiceTypeNodePort,
				Selector: labels,
				Ports: []v1.ServicePort{
					{
						Name:     "spark-client",
						Protocol: v1.ProtocolTCP,
						Port:     7077,
						NodePort: status.ServerNodes[name].Nodeport,
					},
				},
			},
		}

	} else {
		srv = &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				Namespace:       cluster.Namespace,
				OwnerReferences: GetOwners(cluster),
			},
			Spec: v1.ServiceSpec{
				Type:     v1.ServiceTypeNodePort,
				Selector: labels,
				Ports: []v1.ServicePort{
					{
						Name:     "spark-client",
						Protocol: v1.ProtocolTCP,
						Port:     7077,
					},
				},
			},
		}

	}

	if svc, err := k8sutil.CreateService(cluster.Namespace, srv); err == nil {
		for _, Ports := range svc.Spec.Ports {
			status.ServerNodes[nodename].Nodeport = Ports.NodePort
		}
		return svc, nil
	} else {
		return svc, err
	}

}

// 创建一个spark 实例 pod
func CreatePod(cluster *api.SparkCluster, status *api.ClusterStatus, nodename string) (pod *v1.Pod, err error) {

	var volumeid string
	var configmapname string
	spec := cluster.Spec
	labels := GetLabels(cluster.Name, nodename)

	if server, ok := status.ServerNodes[nodename]; ok {
		volumeid = server.VolumeID
		configmapname = server.Configmapname
	} else {
		err := defaulterror.New(fmt.Sprintf("node %q not in the cluster", nodename))
		return nil, err
	}

	labels["spark-group"] = fmt.Sprintf("spark-%s-%s", cluster.Namespace, cluster.Name)

	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            nodename,
			Labels:          labels,
			Namespace:       cluster.Namespace,
			OwnerReferences: GetOwners(cluster),
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: GetTerminationGracePeriodSeconds(),
			RestartPolicy:                 v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:            "spark-server",
					Image:           cluster.Spec.Image,
					ImagePullPolicy: v1.PullIfNotPresent,
					Ports:           []v1.ContainerPort{{ContainerPort: 7077}},
					VolumeMounts: []v1.VolumeMount{
						{Name: "configdir", MountPath: "/usr/local/spark/conf/"},
						{Name: volumeid, MountPath: "/usr/local/spark/logs"},
					},
					Resources: v1.ResourceRequirements{
						Limits: k8sutil.MakeResourceList(spec.Resources.Limits.CPU,
							spec.Resources.Limits.Memory),
						Requests: k8sutil.MakeResourceList(spec.Resources.Requests.CPU,
							spec.Resources.Requests.Memory),
					},
					Env: mkEnv(status.ServerNodes[nodename]),
				},
			},
			Volumes: []v1.Volume{{
				Name: "configdir",
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: configmapname,
						},
					},
				},
			}},

			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
						{
							LabelSelector: &metav1.LabelSelector{
								MatchExpressions: []metav1.LabelSelectorRequirement{
									{
										Key:      "spark-group",
										Operator: metav1.LabelSelectorOpIn,
										Values:   []string{fmt.Sprintf("spark-%s-%s", cluster.Namespace, cluster.Name)},
									},
								},
							},
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			},
		},
	}
	if cluster.Spec.HealthCheck {
		pod.Spec.Containers[0].LivenessProbe = &v1.Probe{
			Handler: v1.Handler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.FromInt(7077),
				},
			},
			InitialDelaySeconds: int32(cluster.Spec.Config.LivenessDelayTimeout),
			TimeoutSeconds:      10,
			PeriodSeconds:       10,
			FailureThreshold:    int32(cluster.Spec.Config.LivenessFailureThreshold),
		}

		pod.Spec.Containers[0].ReadinessProbe = &v1.Probe{
			Handler: v1.Handler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.FromInt(7077),
				},
			},
			InitialDelaySeconds: int32(cluster.Spec.Config.ReadinessDelayTimeout),
			TimeoutSeconds:      10,
			PeriodSeconds:       10,
			FailureThreshold:    int32(cluster.Spec.Config.ReadinessFailureThreshold),
		}
	}

	if len(spec.Volume) == 0 {
		pod.Spec.Volumes = append(pod.Spec.Volumes, k8sutil.MakeEmptyDirVolume(volumeid))
	} else {
		// 使用LVM
		pod.Spec.Volumes = append(pod.Spec.Volumes, v1.Volume{
			Name: volumeid,
			VolumeSource: v1.VolumeSource{
				FlexVolume: &v1.FlexVolumeSource{
					Driver: "sysoperator.pl/lvm",
					FSType: "ext4",
					Options: map[string]string{
						"volumeID":     volumeid,
						"size":         spec.Capactity,
						"volumegroup":  spec.Volume,
						"mountoptions": "relatime,nobarrier",
					},
				},
			},
		})
	}

	// 如果有hostname则绑定，原地重启
	if servD := status.ServerNodes[nodename]; servD != nil && len(servD.Node) > 0 {
		pod.Spec.NodeSelector = map[string]string{nodeSelectHostname: servD.Node}
	}

	k8sutil.SetSparkVersion(pod, cluster.Spec.Version)
	bytes, _ := json.Marshal(pod)
	fmt.Println("----------------------------------------------\n", string(bytes))

	if pod, err = k8sutil.Create(cluster.Namespace, pod); err != nil {
		log.WarnErrorf(err, "create server pod %v", nodename)
		return nil, err
	}
	fmt.Println("---------------------2-------------------------")
	return nil, nil
}

func AddConditions(status *api.ClusterStatus, condType api.ClusterConditionType,
	conditionstatus v1.ConditionStatus,
	reason string, message string) {
	conditions := status.Conditions

	conditions = append(conditions, &api.ClusterCondition{
		Type:    condType,
		Status:  conditionstatus,
		Reason:  reason,
		Message: message,
		LastTransitionTime: metav1.Time{
			Time: time.Now(),
		},
	})

	if len(conditions) > 5 {
		conditions = conditions[4:]
	}

	status.Conditions = conditions
}

func GetOwners(cluster *api.SparkCluster) []metav1.OwnerReference {
	owners := []metav1.OwnerReference{
		*metav1.NewControllerRef(
			cluster,
			schema.GroupVersionKind{
				Group:   api.SchemeGroupVersion.Group,
				Version: api.SchemeGroupVersion.Version,
				Kind:    spark.CrdKind,
			},
		),
	}

	return owners
}

func GetLabels(clusterName, comp string) map[string]string {
	labels := map[string]string{
		"cell": clusterName,
		"app":  "spark",
	}

	if comp != "" {
		labels["component"] = comp
	}

	return labels

}

// GetTerminationGracePeriodSeconds ...
func GetTerminationGracePeriodSeconds() *int64 {
	return &defaultTrminationGracePeriodSeconds
}

func mkEnv(server *api.Server) []v1.EnvVar {

	podEnvs := []v1.EnvVar{
		//角色，根据角色来启动master进程，或者slave进程
		{
			Name: "ROLE", Value: string(server.Role),
		},
		{
			Name: "MASTER", Value: server.Name,
		},
	}

	return podEnvs
}
