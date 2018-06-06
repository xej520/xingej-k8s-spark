/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by informer-gen. DO NOT EDIT.

package v1beta1

import (
	time "time"
	spark_v1beta1 "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis/spark/v1beta1"
	versioned "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/clientset/versioned"
	internalinterfaces "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/informers/externalversions/internalinterfaces"
	v1beta1 "xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client/listers/spark/v1beta1"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// SparkClusterInformer provides access to a shared informer and lister for
// SparkClusters.
type SparkClusterInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1beta1.SparkClusterLister
}

type sparkClusterInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewSparkClusterInformer constructs a new informer for SparkCluster type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewSparkClusterInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredSparkClusterInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredSparkClusterInformer constructs a new informer for SparkCluster type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredSparkClusterInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.SparkV1beta1().SparkClusters(namespace).List(options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.SparkV1beta1().SparkClusters(namespace).Watch(options)
			},
		},
		&spark_v1beta1.SparkCluster{},
		resyncPeriod,
		indexers,
	)
}

func (f *sparkClusterInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredSparkClusterInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *sparkClusterInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&spark_v1beta1.SparkCluster{}, f.defaultInformer)
}

func (f *sparkClusterInformer) Lister() v1beta1.SparkClusterLister {
	return v1beta1.NewSparkClusterLister(f.Informer().GetIndexer())
}
