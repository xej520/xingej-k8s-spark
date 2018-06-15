package k8sutil

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/utils/retry"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// GetPod get pod by name
func GetPod(ns, name string) (*v1.Pod, error) {
	return kubecli.CoreV1().Pods(ns).Get(name, metav1.GetOptions{})
}

// GetPods get pods
func GetPods(ns, cell, component string) ([]v1.Pod, error) {
	set := make(map[string]string)
	if cell != "" {
		set["cell"] = cell
	}
	if component != "" {
		set["component"] = component
	}
	set["app"] = "kafka"
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(set).String(),
	}
	list, err := kubecli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// GetPodsByNamespace get pod by specified namespace
func GetPodsByNamespace(ns string, ls map[string]string) ([]v1.Pod, error) {
	opts := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(ls).String(),
	}
	list, err := kubecli.CoreV1().Pods(ns).List(opts)
	if err != nil {
		return nil, err
	}

	return list.Items, nil
}

// GetPodNames get pod names
func GetPodNames(pods []v1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	names := make([]string, len(pods))
	for i, pod := range pods {
		names[i] = pod.Name
	}

	return names
}

// ListPodNames get pods
func ListPodNames(ns, cell, component string) ([]string, error) {
	pods, err := GetPods(ns, cell, component)
	if err != nil {
		return nil, err
	}
	return GetPodNames(pods), nil
}

// PodWithNodeSelector pod with selector
func PodWithNodeSelector(p *v1.Pod, ns map[string]string) *v1.Pod {
	p.Spec.NodeSelector = ns
	return p
}

// CreatePodByJSON create  pod by json
func CreatePodByJSON(ns string, data []byte, timeout time.Duration, updateFunc func(pod *v1.Pod)) (*v1.Pod, error) {
	pod := &v1.Pod{}
	if err := json.Unmarshal(data, pod); err != nil {
		return nil, err
	}
	updateFunc(pod)
	return CreateAndWaitPod(ns, pod, timeout)
}

// CreateAndWaitPodByJSON create pod by json
func CreateAndWaitPodByJSON(ns string, data []byte, timeout time.Duration) (*v1.Pod, error) {
	pod := &v1.Pod{}
	if err := json.Unmarshal(data, pod); err != nil {
		return nil, err
	}
	return CreateAndWaitPod(ns, pod, timeout)
}

// CreateAndWaitPod create a pod and wait runing
func CreateAndWaitPod(ns string, pod *v1.Pod, timeout time.Duration) (*v1.Pod, error) {
	retPod, err := kubecli.CoreV1().Pods(ns).Create(pod)
	if err != nil {
		return nil, err
	}
	log.Infof("Pod %q created", retPod.GetName())

	retPod, err = waitPodRunning(ns, pod.GetName(), timeout)
	if err != nil {
		return nil, err
	}
	return retPod, err
}

// Create create a pod
func Create(ns string, pod *v1.Pod) (*v1.Pod, error) {
	retPod, err := kubecli.CoreV1().Pods(ns).Create(pod)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, err
		} else {
			return retPod, nil
		}
	}
	log.Infof("Pod %q created", retPod.GetName())
	return retPod, err

}

// PatchPod patch a pod
func PatchPod(ns, name string, timeout time.Duration, updateFunc func(*v1.Pod)) error {
	op, err := kubecli.CoreV1().Pods(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	np := clonePod(op)
	updateFunc(np)
	patchData, err := CreatePatch(op, np, v1.Pod{})
	if err != nil {
		return err
	}

	_, err = kubecli.CoreV1().Pods(ns).Patch(op.GetName(), types.StrategicMergePatchType, patchData)
	if err != nil {
		return err
	}

	time.Sleep(time.Duration(*op.Spec.TerminationGracePeriodSeconds+3) * time.Second)
	_, err = waitPodRunning(ns, op.GetName(), timeout)
	if err != nil {
		return err
	}
	return nil
}

// DeletePods delete pod by name
func DeletePods(ns string, podNames ...string) error {
	deletePolicy := metav1.DeletePropagationForeground
	for _, pName := range podNames {
		err := kubecli.CoreV1().Pods(ns).Delete(pName, &metav1.DeleteOptions{
			PropagationPolicy: &deletePolicy,
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		log.Infof(`Pod "%s" deleted`, pName)
	}
	return nil
}

// DeletePod delete pod
func DeletePod(ns, name string, timeout time.Duration) error {
	deletePolicy := metav1.DeletePropagationForeground
	err := kubecli.CoreV1().Pods(ns).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Warnf("to delete a nonexistent pod %q", name)
			return nil
		}
		return err
	}

	time.Sleep(timeout)
	if p, _ := GetPod(ns, name); p == nil || p.GetName() != name {
		log.Infof("Pod %q deleted", name)
		return nil
	}
	return fmt.Errorf("Pod %q not been deleted", name)
}

// DeletePodsBy delete pods
func DeletePodsBy(ns string, cell, component string) error {
	option := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"cell":      cell,
			"component": component,
		}).String(),
	}
	deletePolicy := metav1.DeletePropagationForeground
	err := kubecli.CoreV1().Pods(ns).DeleteCollection(&metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}, option)
	if err != nil {
		return err
	}

	log.Infof("Pods cell: %q component: %q deleted", cell, component)
	return nil
}

// DeletePodsByLabel delete pod by label
func DeletePodsByLabel(ns string, ls map[string]string) error {
	option := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(ls).String(),
	}
	deletePolicy := metav1.DeletePropagationForeground
	err := kubecli.CoreV1().Pods(ns).DeleteCollection(&metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	}, option)
	if err != nil {
		return err
	}

	log.Infof(strings.Replace(fmt.Sprintf("Pods %q deleted", ls), "map[", "label[", -1))
	return nil
}

// IsPodOk is ok pod
func IsPodOk(pod v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning {
		return false
	}

	for _, c := range pod.Status.Conditions {
		if c.Status != v1.ConditionTrue {
			return false
		}
	}
	return true
}

// IsPodFailed pod is failed or unknown
func IsPodFailed(p *v1.Pod) bool {
	return v1.PodFailed == p.Status.Phase ||
		v1.PodUnknown == p.Status.Phase
}

// IsPodActive pod is active
func IsPodActive(p *v1.Pod) bool {
	return v1.PodSucceeded != p.Status.Phase &&
		v1.PodFailed != p.Status.Phase &&
		p.DeletionTimestamp == nil
}

func IsPodUnknown(p *v1.Pod) bool {
	return v1.PodUnknown == p.Status.Phase
}

// IsPodReady returns true if a pod is ready; false otherwise.
func IsPodReady(pod *v1.Pod) bool {
	return IsPodReadyConditionTrue(pod.Status)
}

// IsPodReadyConditionTrue retruns true if a pod is ready; false otherwise.
func IsPodReadyConditionTrue(status v1.PodStatus) bool {
	condition := GetPodReadyCondition(status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// GetPodReadyCondition extracts the pod ready condition from the given status and returns that.
// Returns nil if the condition is not present.
func GetPodReadyCondition(status v1.PodStatus) *v1.PodCondition {
	_, condition := GetPodCondition(&status, v1.PodReady)
	return condition
}

// GetPodCondition extracts the provided condition from the given status and returns that.
// Returns nil and -1 if the condition is not present, and the index of the located condition.
func GetPodCondition(status *v1.PodStatus, conditionType v1.PodConditionType) (int, *v1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
}

func waitPodRunning(ns, name string, timeout time.Duration) (*v1.Pod, error) {
	var (
		err      error
		retPod   *v1.Pod
		interval = time.Second * 3
	)

	return retPod, retry.Retry(int(timeout/interval), interval, func() (bool, error) {
		retPod, err = kubecli.CoreV1().Pods(ns).Get(name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		switch retPod.Status.Phase {
		case v1.PodRunning:
			return true, nil
		case v1.PodPending:
			for _, c := range retPod.Status.Conditions {
				// no resuces for schedule
				if c.Reason == v1.PodReasonUnschedulable {
					return false, fmt.Errorf("%s:%s", c.Reason, c.Message)
				}
			}
			return false, nil
		default:
			return false, fmt.Errorf("unexpected pod status.phase:%v", retPod.Status.Phase)
		}
	})
}

func clonePod(pod *v1.Pod) *v1.Pod {
	nPod := &v1.Pod{}
	err := convert.Convert(&pod, nPod, 0, nil)
	if err != nil {
		log.Panic(err)
	}

	return nPod
}
