package k8sutil

import (
	"k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	rbacv1beta1 "k8s.io/api/rbac/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreateDaemonset create daemonset
func CreateDaemonset(ns string, ds *v1beta1.DaemonSet) (*v1beta1.DaemonSet, error) {
	return kubecli.ExtensionsV1beta1().DaemonSets(ns).Create(ds)
}

// CreateServiceAccount create service account
func CreateServiceAccount(ns, name string) error {
	serviceAccount := v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
	}
	_, err := kubecli.CoreV1().ServiceAccounts(ns).Create(&serviceAccount)
	return err
}

// CreateClusterRoleBinding create role binding
func CreateClusterRoleBinding(ns, name string) error {
	binding := rbacv1beta1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		RoleRef: rbacv1beta1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "cluster-admin",
		},
		Subjects: []rbacv1beta1.Subject{
			{
				Kind:      rbacv1beta1.ServiceAccountKind,
				Name:      name,
				Namespace: ns,
			},
		},
	}

	_, err := kubecli.RbacV1beta1().ClusterRoleBindings().Create(&binding)
	if err != nil {
		return err
	}
	return nil
}
