package crd

import (
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/crd/sparkapplication"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/util"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"reflect"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 注册SparkApplication类型的CRD
func RegisterCRDForSparkApplication(endpoint, configPath string) {

	fmt.Printf("----> 开始注册CRD")

	// 1、创建extension 客户端
	apiExtensionClient := util.KubeExtensionClient(endpoint, configPath)

	// 2、创建SparkApplication对应的CRD对象
	sparkAppCrdObject := sparkapplication.GetCRDObject()

	//3、校验是否已经注册过CRD
	existing, err := apiExtensionClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(sparkAppCrdObject.Name, metav1.GetOptions{})

	if err != nil && !apierrors.IsNotFound(err) {
		// Failed to get the CRD object and the failure was not because the object cannot be found.
		return
	}

	if err == nil && existing != nil {
		// 说明已经注册过了
		// 就不再进行重复注册，当前处理逻辑：退出即可

		if !reflect.DeepEqual(existing.Spec, sparkAppCrdObject.Spec) {
			existing.Spec = sparkAppCrdObject.Spec
			glog.Infof("Updating CustomResourceDefinition %s", sparkAppCrdObject.Name)
			if _, err := apiExtensionClient.ApiextensionsV1beta1().CustomResourceDefinitions().Update(existing); err != nil  {
				fmt.Printf("更新CRD失败了 %s ", sparkAppCrdObject.Name)
				fmt.Printf("失败原因:\n ", err)

				apiExtensionClient.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(sparkAppCrdObject.Name,metav1.NewDeleteOptions(0))
				fmt.Printf("删除已经注册过的%s\n CRD ", sparkAppCrdObject.Name)

				return
			}
		}


		fmt.Printf("已经注册过 %s CRD了", sparkAppCrdObject.Name)
		return
	}

	// 4、开始注册到k8s
	util.RegisterCRD(apiExtensionClient, sparkAppCrdObject)

	fmt.Printf("成功注册 %s CRD", sparkAppCrdObject.Name)

}
