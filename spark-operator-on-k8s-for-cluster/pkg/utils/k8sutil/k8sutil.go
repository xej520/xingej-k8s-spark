package k8sutil

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"

	appv1 "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	aiperrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	conver "k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"bonc.com/kafka-operator/pkg/generated/clientset/versioned"
	"bonc.com/kafka-operator/pkg/utils/constants"
)

const (
	kubePublicNamespace        = "kube-public"
	kafkaVersionAnnotationsKey = "kafka.version"
)

var convert = conver.NewConverter(conver.DefaultNameFunc)

var (
	// MasterHost k8s master host
	MasterHost string
	// Kubeconfig k8s kube config
	Kubeconfig string

	// OperatorNamespace defalult Operate namespace
	OperatorNamespace string

	kubecli kubernetes.Interface

	// ErrNotDeleted ...
	ErrNotDeleted = errors.New("not deleted")
)

func init() {
	OperatorNamespace = os.Getenv(constants.EnvOperatorPodNamespace)
	if len(OperatorNamespace) == 0 {
		OperatorNamespace = "default"
	}
	MasterHost = os.Getenv(constants.EnvK8sMasterHost)
	Kubeconfig = os.Getenv(constants.EnvK8sKubeConfig)
	log.Infof("kafka-operator current namespace is %s", OperatorNamespace)
}

// MustInit init k8s
func MustInit() {
	if len(MasterHost) > 0 {
		log.Infof("kubernetes config info, master host: %v, kube conf: %v", MasterHost, Kubeconfig)
	}
	kubecli = MustNewKubeClient()
	if err := createNamespace(OperatorNamespace); err != nil && !IsKubernetesResourceAlreadyExistError(err) {
		log.PanicErrorf(err, "Init kafka-operator k8s namespace %s", OperatorNamespace)
	}
}

// MustNewKubeClient new k8s client
func MustNewKubeClient() kubernetes.Interface {
	cfg, err := ClusterConfig()
	if err != nil {
		log.Panic(err)
	}
	return kubernetes.NewForConfigOrDie(cfg)
}

// MustNewCodisExtClient new kafka cluster client
func MustNewKafkaExtClient() versioned.Interface {
	cfg, err := ClusterConfig()
	if err != nil {
		log.Panic(err)
	}
	return versioned.NewForConfigOrDie(cfg)
}

// ClusterConfig get k8s cluster config
func ClusterConfig() (*rest.Config, error) {
	if len(MasterHost) > 0 || Kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(MasterHost, Kubeconfig)
	}
	return inClusterConfig()
}

func inClusterConfig() (*rest.Config, error) {
	if len(os.Getenv("KUBERNETES_SERVICE_HOST")) == 0 {
		addrs, err := net.LookupHost("kubernetes.default.svc")
		if err != nil {
			log.Panic(err)
		}
		os.Setenv("KUBERNETES_SERVICE_HOST", addrs[0])
	}
	if len(os.Getenv("KUBERNETES_SERVICE_PORT")) == 0 {
		os.Setenv("KUBERNETES_SERVICE_PORT", "443")
	}
	return rest.InClusterConfig()
}

// NewRESTClient return a restClient
func NewRESTClient() rest.Interface {
	return kubecli.CoreV1().RESTClient()
}

// GetKube get a kubecli
func GetKube() kubernetes.Interface {
	return kubecli
}

// IsKubernetesResourceAlreadyExistError whether it is resource error
func IsKubernetesResourceAlreadyExistError(err error) bool {
	return aiperrs.IsAlreadyExists(err)
}

// IsKubernetesResourceNotFoundError whether it is resource not found error
func IsKubernetesResourceNotFoundError(err error) bool {
	return aiperrs.IsNotFound(err)
}

// CreatePatch create a patch
func CreatePatch(original, modified, dataStruct interface{}) ([]byte, error) {
	oldData, err := json.Marshal(original)
	if err != nil {
		return nil, err
	}

	newData, err := json.Marshal(modified)
	if err != nil {
		return nil, err
	}
	return strategicpatch.CreateTwoWayMergePatch(oldData, newData, dataStruct)
}

// CascadeDeleteOptions return DeleteOptions with cascade
func CascadeDeleteOptions(gracePeriodSeconds int64) *metav1.DeleteOptions {
	return &metav1.DeleteOptions{
		GracePeriodSeconds: func(t int64) *int64 { return &t }(gracePeriodSeconds),
		PropagationPolicy: func() *metav1.DeletionPropagation {
			foreground := metav1.DeletePropagationForeground
			return &foreground
		}(),
	}
}

// DeleteOptions return DeleteOptions with gracePeriodSeconds
func DeleteOptions(gracePeriodSeconds int64) *metav1.DeleteOptions {
	return &metav1.DeleteOptions{
		GracePeriodSeconds: func(t int64) *int64 { return &t }(gracePeriodSeconds),
	}
}

// l2 into l1. Conflicting label will be skipped.
func mergeLabels(l1, l2 map[string]string) {
	for k, v := range l2 {
		if _, ok := l1[k]; ok {
			continue
		}
		l1[k] = v
	}
}

// GetNodesExternalIP get node external IP
func GetNodesExternalIP(selector map[string]string) ([]string, error) {
	option := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(selector).String(),
	}
	nodes, err := kubecli.CoreV1().Nodes().List(option)
	if err != nil {
		return nil, err
	}
	var ips []string
	for _, node := range nodes.Items {
		addrs := node.Status.Addresses
		for _, addr := range addrs {
			if addr.Type == v1.NodeInternalIP {
				ips = append(ips, addr.Address)
				break
			}
		}
	}
	return ips, nil
}

// GetK8sEtcdIP get k8s etcd ip
func GetK8sEtcdIP() (string, error) {
	ls := map[string]string{
		"component": "etcd",
		"tier":      "control-plane",
	}
	pods, err := GetPodsByNamespace("kube-system", ls)
	if err != nil {
		return "", err
	}
	if len(pods) != 1 {
		return "", fmt.Errorf("get multi etcd %s", GetPodNames(pods))
	}
	return pods[0].Status.PodIP, nil
}

// GetCodisVersion get codis version
func GetCodisVersion(i interface{}) string {
	switch v := i.(type) {
	case *v1.Pod:
		return v.Annotations[kafkaVersionAnnotationsKey]
	case *v1.ReplicationController:
		return v.Annotations[kafkaVersionAnnotationsKey]
	case *appv1.StatefulSet:
		return v.Annotations[kafkaVersionAnnotationsKey]
	}
	return ""
}

// SetKafkaVersion set kafka version
func SetKafkaVersion(i interface{}, version string) {
	switch v := i.(type) {
	case *v1.Pod:
		if len(v.Annotations) < 1 {
			v.Annotations = make(map[string]string)
		}
		v.Annotations[kafkaVersionAnnotationsKey] = version
	case *v1.ReplicationController:
		if len(v.Annotations) < 1 {
			v.Annotations = make(map[string]string)
		}
		v.Annotations[kafkaVersionAnnotationsKey] = version

		if len(v.Spec.Template.Annotations) < 1 {
			v.Spec.Template.Annotations = make(map[string]string)
		}
		v.Spec.Template.Annotations[kafkaVersionAnnotationsKey] = version
	case *appv1.StatefulSet:
		if len(v.Annotations) < 1 {
			v.Annotations = make(map[string]string)
		}
		v.Annotations[kafkaVersionAnnotationsKey] = version
		v.Spec.Template.Annotations[kafkaVersionAnnotationsKey] = version
	}
}

// MakeEmptyDirVolume make a empty dir
func MakeEmptyDirVolume(name string) v1.Volume {
	return v1.Volume{
		Name:         name,
		VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
	}
}

// MakePodIPEnvVar make pod ip ENV
func MakePodIPEnvVar() v1.EnvVar {
	return v1.EnvVar{
		Name: "POD_IP",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	}
}

// MakeResourceList make a resource list
func MakeResourceList(cpu, mem string) v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse(cpu),
		v1.ResourceMemory: resource.MustParse(mem),
	}
}
