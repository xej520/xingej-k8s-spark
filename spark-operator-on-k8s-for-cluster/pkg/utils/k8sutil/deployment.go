package k8sutil

import (
	"encoding/json"
	"time"

	"bonc.com/kafka-operator/pkg/utils/retry"

	"github.com/CodisLabs/codis/pkg/utils/log"

	extv1 "k8s.io/api/extensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CreateDeploymentByJSON create a Deployment by json
func CreateDeploymentByJSON(ns string, data []byte, timeout time.Duration,
	updateFun func(*extv1.Deployment)) (*extv1.Deployment, error) {
	sfs := &extv1.Deployment{}
	if err := json.Unmarshal(data, sfs); err != nil {
		return nil, err
	}
	updateFun(sfs)
	retSt, err := CreateAndWaitDeployment(ns, sfs, timeout)
	if err != nil {
		return nil, err
	}
	log.Infof("Deployment %q created", retSt.GetName())
	return retSt, nil
}

// CreateAndWaitDeployment create a Deployment
func CreateAndWaitDeployment(ns string, dep *extv1.Deployment, timeout time.Duration) (*extv1.Deployment, error) {
	//retDep, err := kubecli.AppsV1().Deployments(Namespace).Create(dep)
	retDep, err := kubecli.ExtensionsV1beta1().Deployments(ns).Create(dep)
	if err != nil {
		return nil, err
	}

	interval := 3 * time.Second
	err = retry.Retry(int(timeout/interval), interval, func() (bool, error) {
		retDep, err = kubecli.ExtensionsV1beta1().Deployments(ns).Get(retDep.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if retDep.Status.AvailableReplicas != *dep.Spec.Replicas {
			return false, nil
		}

		return true, nil
	})
	log.Infof("Deployment %q created", retDep.Name)

	return retDep, nil
}

// PatchDeployment patch a Deployment
func PatchDeployment(ns, name string, updateFunc func(*extv1.Deployment)) error {
	oDep, err := kubecli.ExtensionsV1beta1().Deployments(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	nDep := cloneDep(oDep)
	updateFunc(nDep)
	patchData, err := CreatePatch(oDep, nDep, extv1.Deployment{})
	if err != nil {
		return err
	}
	_, err = kubecli.ExtensionsV1beta1().Deployments(ns).Patch(name, types.StrategicMergePatchType, patchData)
	return err
}

// DeleteDeployment delete a Deployment
func DeleteDeployment(ns, name string) error {
	deletePolicy := metav1.DeletePropagationForeground
	err := kubecli.ExtensionsV1beta1().Deployments(ns).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	log.Infof("Deployment %q deleted", name)
	return nil
}

func cloneDep(dep *extv1.Deployment) *extv1.Deployment {
	nDep := &extv1.Deployment{}
	if err := convert.Convert(&dep, nDep, 0, nil); err != nil {
		log.Panic(err)
	}

	return nDep
}
