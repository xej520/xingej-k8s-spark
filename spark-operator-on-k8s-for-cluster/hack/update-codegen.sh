#!/bin/bash

#是 进入到自己工程中vendor下的code-generator目录
#
cd /usr/local/go/gopath/src/xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/vendor/k8s.io/code-generator

chmod +x generate-groups.sh

bash generate-groups.sh "all" \
xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/client  \
xingej-go/xingej-k8s-spark/spark-operator-on-k8s-for-cluster/pkg/apis  \
"spark:v1beta1"


