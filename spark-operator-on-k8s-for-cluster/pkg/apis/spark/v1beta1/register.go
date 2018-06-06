package v1beta1


import (
	api "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme

	// SchemeGroupVersion Spark scheme
	SchemeGroupVersion = schema.GroupVersion{Group: api.CrdGroup, Version: api.CrdVersion}
)

// Resource gets an SparkCluster GroupResource for a specified resource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// addKnownTypes adds the set of types defined in this package to the supplied scheme.
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypeWithName(SchemeGroupVersion.WithKind(api.CrdKind), &SparkCluster{})
	s.AddKnownTypes(SchemeGroupVersion,
		&SparkCluster{},
		&SparkClusterList{},
	)
	metav1.AddToGroupVersion(s, SchemeGroupVersion)
	return nil
}
