package k8sutil

import (
	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateConfigmap create a configmap
func CreateConfigmap(ns, name string, data map[string]string) error {
	configMap := v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Data: data,
	}

	_, err := kubecli.CoreV1().ConfigMaps(ns).Create(&configMap)
	return err
}

// CreateConfigmapWithOwners create a configmap
func CreateConfigmapWithOwners(ns, name string, owner []metav1.OwnerReference, data map[string]string) error {
	configMap := v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			OwnerReferences: owner,
		},
		Data: data,
	}

	_, err := kubecli.CoreV1().ConfigMaps(ns).Create(&configMap)
	return err
}

// GetConfigmap get config map
func GetConfigmap(ns, name string) (map[string]string, error) {
	configMap, err := kubecli.CoreV1().ConfigMaps(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return configMap.Data, nil
}

// UpdateConfigmap update config map
func UpdateConfigmap(ns, name string, data map[string]string) error {
	configMap, err := kubecli.CoreV1().ConfigMaps(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	configMap.Data = data
	_, err = kubecli.CoreV1().ConfigMaps(ns).Update(configMap)
	return err
}

// DeleteConfigmap delete configmap
func DeleteConfigmap(ns, name string) error {
	deletePolicy := metav1.DeletePropagationForeground
	if err := kubecli.CoreV1().ConfigMaps(ns).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}); err != nil {
		return err
	}

	log.Infof("Configmap %q deleted", name)
	return nil
}
