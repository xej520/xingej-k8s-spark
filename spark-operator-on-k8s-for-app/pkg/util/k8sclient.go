package util

import (
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/client-go/rest"
	"fmt"
)

// 通过配置文件，其实，也就是来获当前环境的上下文
// Clientset 包含多个版本的client
func GetK8sClientByClientset(endpoint string) (*kubernetes.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags(endpoint,
		"kubeconfig/config")
	if err != nil {
		return nil, err
	}
	clientset, e := kubernetes.NewForConfig(config)

	if e != nil {
		return nil, err
	}
	return clientset, nil
}

func GetApiExtensionClient(endpoint string) apiextensionsclient.Interface {
	config, err := clientcmd.BuildConfigFromFlags(endpoint,
		"kubeconfig/config")

	if err != nil {
		return nil
	}

	apiExtensionsClient := apiextensionsclient.NewForConfigOrDie(config)

	return apiExtensionsClient

}

// 获取config对象
func ClusterConfig(endpoint , configPath string) (*rest.Config, error) {

	return clientcmd.BuildConfigFromFlags(endpoint, configPath)
}

// e获取
func KubeExtensionClient(endpoint, configPath string) apiextensionsclient.Interface {
	cfg, err := ClusterConfig(endpoint, configPath)

	if err != nil {
		fmt.Println("--->根据config来生成对应的config对象失败!")
		return nil
	}

	return apiextensionsclient.NewForConfigOrDie(cfg)
}

func RegisterCRD(
	clientset apiextensionsclient.Interface,
	crd *apiextensionsv1beta1.CustomResourceDefinition) error {

	if _, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd); err != nil {
		fmt.Println("-------------创建自定义类型的对象---失败了------------>:\n", err)
		return nil
	}

	fmt.Println("--------创建自定义类型的对象---OK-----")

	return nil
}


