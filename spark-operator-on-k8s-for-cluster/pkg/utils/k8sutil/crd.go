package k8sutil

import (
	"fmt"
	"strings"
	"time"

	api "bonc.com/kafka-operator/pkg/apis/kafka/v1beta1"
	"bonc.com/kafka-operator/pkg/utils/retry"

	"github.com/CodisLabs/codis/pkg/utils/log"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateCRD create a crd
func CreateCRD(kind string) error {
	clientset := MustNewKubeExtClient()

	singular := strings.ToLower(kind)
	plural := singular + "s"

	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s.%s", plural, api.SchemeGroupVersion.Group),
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   api.SchemeGroupVersion.Group,
			Version: api.SchemeGroupVersion.Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Singular: singular,
				Plural:   plural,
				Kind:     kind,
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		return nil
	}

	return WaitCRDReady(kind, clientset)
}

// WaitCRDReady wait a crd runing
func WaitCRDReady(kind string, clientset apiextensionsclient.Interface) error {
	name := fmt.Sprintf("%ss.%s", strings.ToLower(kind), api.SchemeGroupVersion.Group)
	notFountWait := 0
	err := retry.Retry(20, 5*time.Second, func() (bool, error) {
		crd, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				notFountWait++
				if notFountWait > 5 {
					return false, err
				}
				return false, nil
			}
			return false, err
		}

		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, nil
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					return false, fmt.Errorf("Name conflict: %v", cond.Reason)
				}
			}
		}

		return false, nil
	})

	if err != nil {
		return fmt.Errorf("wait CRD created failed: %v", err)
	}
	return nil
}

// MustNewKubeExtClient get ext client
func MustNewKubeExtClient() apiextensionsclient.Interface {
	cfg, err := ClusterConfig()
	if err != nil {
		log.Panic(err)
	}

	return apiextensionsclient.NewForConfigOrDie(cfg)
}
