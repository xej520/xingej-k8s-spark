package k8sutil

import (
	"testing"
	"time"
)

func TestDeletePod(t *testing.T) {
	err := DeletePod("default", "aaa", time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
}
