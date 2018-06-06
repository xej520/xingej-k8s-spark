package k8sutil

import (
	"encoding/json"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateServiceByJSON create service by json data
func CreateServiceByJSON(ns string, data []byte) (*v1.Service, error) {
	srv := &v1.Service{}
	if err := json.Unmarshal(data, srv); err != nil {
		return nil, err
	}

	return CreateService(ns, srv)
}

// CreateService create service
func CreateService(ns string, srv *v1.Service) (*v1.Service, error) {
	retSrv, err := kubecli.CoreV1().Services(ns).Create(srv)
	if err != nil {
		return retSrv, err
	}

	log.Infof("Service %s created", retSrv.GetName())
	return retSrv, nil
}

// GetService get a service
func GetService(ns, name string) (*v1.Service, error) {
	return kubecli.CoreV1().Services(ns).Get(name, metav1.GetOptions{})
}

// UpdateService update service
func UpdateService(ns, name string, updateFunc func(*v1.Service)) (*v1.Service, error) {
	srv, err := kubecli.CoreV1().Services(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	updateFunc(srv)

	return kubecli.CoreV1().Services(ns).Update(srv)
}

// DeleteServices delete service
func DeleteServices(ns string, names ...string) error {
	for _, name := range names {
		kubecli.CoreV1().Services(ns).Delete(name, &metav1.DeleteOptions{})
		log.Infof("Service %q deleted", name)
	}
	return nil
}
