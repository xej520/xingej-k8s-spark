package k8sutil

import (
	"github.com/CodisLabs/codis/pkg/utils/log"

	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createNamespace(name string) error {
	ns := &v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: name,
		},
	}

	retNs, err := kubecli.CoreV1().Namespaces().Create(ns)
	if err != nil {
		return err
	}
	log.Infof("Namespace %s created", retNs.Name)
	return nil
}

func deleteNamespace(name string) error {
	err := kubecli.CoreV1().Namespaces().Delete(name, meta_v1.NewDeleteOptions(0))
	if err != nil {
		return err
	}

	log.Warnf(`Namespace "%s" deleted`, name)
	return nil
}
