package k8sutil

import (
	"encoding/json"
	"time"

	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/retry"
	"github.com/CodisLabs/codis/pkg/utils/log"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CreateRcByJSON create a rc by json data
func CreateRcByJSON(ns string, data []byte, timeout time.Duration,
	updateFun func(*v1.ReplicationController)) (*v1.ReplicationController, error) {
	rc := &v1.ReplicationController{}
	if err := json.Unmarshal(data, rc); err != nil {
		return nil, err
	}
	updateFun(rc)
	retRc, err := CreateAndWaitRc(ns, rc, timeout)
	if err != nil {
		return nil, err
	}
	log.Infof("ReplicationController %q created", retRc.GetName())
	return retRc, nil
}

// CreateAndWaitRc create a rc and wait ok
func CreateAndWaitRc(ns string, rc *v1.ReplicationController, timeout time.Duration) (*v1.ReplicationController, error) {
	retRc, err := kubecli.CoreV1().ReplicationControllers(ns).Create(rc)
	if err != nil {
		return nil, err
	}

	interval := 3 * time.Second
	err = retry.Retry(int(timeout/interval), interval, func() (bool, error) {
		retRc, err = kubecli.CoreV1().ReplicationControllers(ns).Get(rc.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if retRc.Status.AvailableReplicas != *rc.Spec.Replicas {
			return false, nil
		}

		return true, nil
	})
	log.Infof("ReplicationController %q created", retRc.Name)
	return retRc, nil
}

// PatchRc patch a rc
func PatchRc(ns, name string, updateFunc func(*v1.ReplicationController)) error {
	oRc, err := kubecli.CoreV1().ReplicationControllers(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	nRc := cloneRc(oRc)
	updateFunc(nRc)
	patchData, err := CreatePatch(oRc, nRc, v1.ReplicationController{})
	if err != nil {
		return err
	}
	_, err = kubecli.CoreV1().ReplicationControllers(ns).Patch(name, types.StrategicMergePatchType, patchData)
	return err
}

// DeleteRc delete a rc
func DeleteRc(ns, name string) error {
	err := kubecli.CoreV1().ReplicationControllers(ns).Delete(name, metav1.NewDeleteOptions(0))
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	log.Infof("ReplicationController %q deleted", name)
	return nil
}

// ScaleReplicationController scale a rc
func ScaleReplicationController(ns, name string, replicas int) error {
	var nums int32
	if replicas < 0 {
		nums = 0
	} else {
		nums = int32(replicas)
	}

	return PatchRc(ns, name, func(rc *v1.ReplicationController) {
		rc.Spec.Replicas = &nums
	})
}

func cloneRc(rc *v1.ReplicationController) *v1.ReplicationController {
	nRc := &v1.ReplicationController{}
	if err := convert.Convert(&rc, nRc, 0, nil); err != nil {
		log.Panic(err)
	}

	return nRc
}
