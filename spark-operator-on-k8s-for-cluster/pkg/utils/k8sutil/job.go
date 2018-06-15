package k8sutil

import (
	"time"

	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/retry"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CreateAndWaitJob create job and wait runing
func CreateAndWaitJob(ns string, job *v1.Job, timeout time.Duration) (*v1.Job, error) {
	retJob, err := kubecli.BatchV1().Jobs(ns).Create(job)
	if err != nil {
		return nil, err
	}

	interval := time.Second
	err = retry.Retry(int(timeout/interval), interval, func() (bool, error) {
		retJob, err = kubecli.BatchV1().Jobs(ns).Get(job.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		switch retJob.Status.Active {
		case 1:
			return true, nil
		default:
			return false, nil
		}
	})
	log.Infof("Job '%q' created", retJob.GetName())
	return retJob, nil
}

// PatchJob patch a job
func PatchJob(ns, name string, updateFunc func(job *v1.Job)) error {
	oJob, err := kubecli.BatchV1().Jobs(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	nJob := cloneJob(oJob)
	updateFunc(nJob)
	patchData, err := CreatePatch(oJob, nJob, v1.Job{})
	if err != nil {
		return err
	}
	_, err = kubecli.BatchV1().Jobs(ns).Patch(name, types.StrategicMergePatchType, patchData)
	return err
}

// DeleteJob delete a job
func DeleteJob(ns, name string) error {
	deletePolicy := metav1.DeletePropagationForeground
	err := kubecli.BatchV1().Jobs(ns).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	err = DeletePodsByLabel(ns, map[string]string{"job-name": name})
	if err != nil {
		return err
	}
	return nil
}

// GetJob get a job
func GetJob(ns, name string) (*v1.Job, error) {
	return kubecli.BatchV1().Jobs(ns).Get(name, metav1.GetOptions{})
}

func cloneJob(rc *v1.Job) *v1.Job {
	nRc := &v1.Job{}
	if err := convert.Convert(&rc, nRc, 0, nil); err != nil {
		log.Panic(err)
	}

	return nRc
}
