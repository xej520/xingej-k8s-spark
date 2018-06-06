package sparkapplication

import (
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io"
	"reflect"
	"xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-app/pkg/apis/spark/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

//CRD metadata
const (
	Plural    = "sparkapplications"
	Singular  = "sparkapplication"
	ShortName = "sparkapp"
	Group     = sparkoperator.GroupName
	Version   = "v1beta1"
	FullName  = Plural + "." + Group
)

const INTEGER = "integer"

func GetCRDObject() *apiextensionsv1beta1.CustomResourceDefinition {
	return &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: v1.ObjectMeta{
			Name: FullName,
		},

		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural:     Plural,
				Singular:   Singular,
				ShortNames: []string{ShortName},
				Kind:       reflect.TypeOf(v1beta1.SparkApplication{}).Name(),
			},
			Validation: getCustomResourceValidation(),
		},
	}

}

func float64Ptr(f float64) *float64 {
	return &f
}

// 自定义资源的验证
func getCustomResourceValidation() *apiextensionsv1beta1.CustomResourceValidation {
	return &apiextensionsv1beta1.CustomResourceValidation{
		OpenAPIV3Schema: &apiextensionsv1beta1.JSONSchemaProps{
			Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
				"spec": {
					Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
						"type": {
							Enum: []apiextensionsv1beta1.JSON{
								{Raw: []byte(`"Java"`)},
								{Raw: []byte(`"Scala"`)},
								{Raw: []byte(`"Python"`)},
								{Raw: []byte(`"R"`)},
							},
						},

						"mode": {
							Enum: []apiextensionsv1beta1.JSON{
								{Raw: []byte(`"cluster"`)},
								{Raw: []byte(`"client"`)},
							},
						},
						"driver": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"cores": {
									Type:             "number",
									Minimum:          float64Ptr(0),
									ExclusiveMinimum: true,
								},
								"podName": {
									Pattern: "[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
								},
							},
						},
						"executor": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"cores": {
									Type:    INTEGER,
									Minimum: float64Ptr(1),
								},
								"instances": {
									Type:    INTEGER,
									Minimum: float64Ptr(1),
								},
							},
						},
						"deps": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"downloadTimeout": {
									Type:    INTEGER,
									Minimum: float64Ptr(1),
								},
								"maxSimultaneousDownloads": {
									Type:    INTEGER,
									Minimum: float64Ptr(1),
								},
							},
						},
						"restartPolicy": {
							Enum: []apiextensionsv1beta1.JSON{
								{Raw: []byte(`"Never"`)},
								{Raw: []byte(`"OnFailure"`)},
								{Raw: []byte(`"Always"`)},
							},
						},
						"maxSubmissionRetries": {
							Type:    INTEGER,
							Minimum: float64Ptr(1),
						},
						"submissionRetryInterval": {
							Type:    INTEGER,
							Minimum: float64Ptr(1),
						},
					},
				},
			},
		},
	}

}
