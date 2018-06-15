package k8sutil

import (
	"fmt"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/constants"
)

func TestMain(m *testing.M) {
	MasterHost = "https://172.16.26.4:6443"
	Kubeconfig = "/Users/MarkYang/.devkube/config"
	MustInit()
	kubecli = MustNewKubeClient()
	os.Exit(m.Run())
}

func TestMustNewKubeClient(t *testing.T) {
	kc := MustNewKubeClient()
	pods, err := kc.CoreV1().Pods("").List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))
}

func TestGetNodesIP(t *testing.T) {
	sel := map[string]string{
		"node-type": "minikube",
	}
	ips, err := GetNodesExternalIP(sel)
	if err != nil {
		t.Errorf("%v", err)
	}
	fmt.Printf("size:%d %s\n", len(ips), ips)
}

func TestGetEnv(t *testing.T) {
	MasterHost := os.Getenv(constants.EnvK8sMasterHost)
	Kubeconfig := os.Getenv(constants.EnvK8sKubeConfig)

	fmt.Printf("%v-%v\r\n", MasterHost, Kubeconfig)
}
