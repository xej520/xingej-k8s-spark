package k8sutil

import (
	"encoding/json"
	"time"

	"bonc.com/kafka-operator/pkg/utils/retry"

	"github.com/CodisLabs/codis/pkg/utils/log"

	appv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CreateStatefulByJSON create a StatefulSet by json
func CreateStatefulByJSON(ns string, data []byte, timeout time.Duration,
	updateFun func(*appv1.StatefulSet)) (*appv1.StatefulSet, error) {
	sfs := &appv1.StatefulSet{}
	if err := json.Unmarshal(data, sfs); err != nil {
		return nil, err
	}
	updateFun(sfs)
	retSt, err := CreateAndWaitStateful(ns, sfs, timeout)
	if err != nil {
		return nil, err
	}
	log.Infof("StatefulSet %q created", retSt.GetName())
	return retSt, nil
}

// CreateAndWaitStateful create a StatefulSet
func CreateAndWaitStateful(ns string, sfs *appv1.StatefulSet, timeout time.Duration) (*appv1.StatefulSet, error) {
	retSfs, err := kubecli.AppsV1().StatefulSets(ns).Create(sfs)
	if err != nil {
		return nil, err
	}

	interval := 3 * time.Second
	err = retry.Retry(int(timeout/interval), interval, func() (bool, error) {
		retSfs, err = kubecli.AppsV1().StatefulSets(ns).Get(retSfs.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if retSfs.Status.CurrentReplicas != *sfs.Spec.Replicas {
			return false, nil
		}

		return true, nil
	})
	log.Infof("StatefulSet %q created", retSfs.Name)

	return retSfs, nil
}

// PatchStateful patch a StatefulSet
func PatchStateful(ns, name string, updateFunc func(*appv1.StatefulSet)) error {
	oSfs, err := kubecli.AppsV1().StatefulSets(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	nSfs := cloneSfs(oSfs)
	updateFunc(nSfs)
	patchData, err := CreatePatch(oSfs, nSfs, appv1.StatefulSet{})
	if err != nil {
		return err
	}
	_, err = kubecli.AppsV1().StatefulSets(ns).Patch(name, types.StrategicMergePatchType, patchData)
	return err
}

// DeleteStateful delete a StatefulSet
func DeleteStateful(ns, name string) error {
	deletePolicy := metav1.DeletePropagationForeground
	err := kubecli.AppsV1().StatefulSets(ns).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	log.Infof("StatefulSet %q deleted", name)
	return nil
}

func cloneSfs(sfs *appv1.StatefulSet) *appv1.StatefulSet {
	nSfs := &appv1.StatefulSet{}
	if err := convert.Convert(&sfs, nSfs, 0, nil); err != nil {
		log.Panic(err)
	}

	return nSfs
}
