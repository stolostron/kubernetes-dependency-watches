package client_test

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/stolostron/kubernetes-dependency-watches/client"
)

type reconciler struct{}

func (r *reconciler) Reconcile(ctxTest context.Context, watcher client.ObjectIdentifier) (reconcile.Result, error) {
	fmt.Printf("An object that this object (%s) was watching was updated\n", watcher)

	return reconcile.Result{}, nil
}

func ExampleDynamicWatcher() {
	// Start a test Kubernetes API.
	testEnv := envtest.Environment{}

	k8sConfig, err := testEnv.Start()
	if err != nil {
		panic(err)
	}

	defer func() {
		err := testEnv.Stop()
		if err != nil {
			klog.Error(err, "failed to stop the test Kubernetes API")
		}
	}()

	// Create the dynamic watcher.
	dynamicWatcher, err := client.New(k8sConfig, &reconciler{}, nil)
	if err != nil {
		panic(err)
	}

	// A context that is canceled after a SIGINT signal is received.
	parentCtx := ctrl.SetupSignalHandler()
	// Create a child context that can be explicitly canceled.
	ctx, cancel := context.WithCancel(parentCtx)

	// Start the dynamic watcher in a separate goroutine to not block the main goroutine.
	go func() {
		err := dynamicWatcher.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()

	// Wait until the dynamic watcher has started.
	<-dynamicWatcher.Started()

	// Simulate something canceling the context in 5 seconds so that the example exits.
	go func() {
		time.Sleep(5 * time.Second)

		cancel()
	}()

	watcher := client.ObjectIdentifier{
		Group:     "",
		Version:   "v1",
		Kind:      "ConfigMap",
		Namespace: "default",
		Name:      "watcher",
	}
	watched1 := client.ObjectIdentifier{
		Group:     "",
		Version:   "v1",
		Kind:      "Secret",
		Namespace: "default",
		Name:      "watched1",
	}
	watched2 := client.ObjectIdentifier{
		Group:     "",
		Version:   "v1",
		Kind:      "Secret",
		Namespace: "default",
		Name:      "watched2",
	}

	// Get notified about watcher when watched1 or watched2 is updated.
	err = dynamicWatcher.AddOrUpdateWatcher(watcher, watched1, watched2)
	if err != nil {
		panic(err)
	}

	// Output:
}
