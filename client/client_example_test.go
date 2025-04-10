package client_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	klog "k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/stolostron/kubernetes-dependency-watches/client"
)

type reconciler struct{}

func (r *reconciler) Reconcile(_ context.Context, watcher client.ObjectIdentifier) (reconcile.Result, error) {
	//nolint: forbidigo
	fmt.Printf("An object that this object (%s) was watching was updated\n", watcher)

	return reconcile.Result{}, nil
}

type ctrlRuntimeReconciler struct{}

func (r *ctrlRuntimeReconciler) Reconcile(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
	//nolint: forbidigo
	fmt.Printf("The following reconcile request was received: %v\n", req)

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
			klog.Errorf("failed to stop the test Kubernetes API, error: %v", err)
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

	// Run until the context is canceled.
	<-ctx.Done()

	// Output:
}

func ExampleNewControllerRuntimeSource() {
	// Start a test Kubernetes API.
	testEnv := envtest.Environment{}

	k8sConfig, err := testEnv.Start()
	if err != nil {
		panic(err)
	}

	defer func() {
		err := testEnv.Stop()
		if err != nil {
			klog.Errorf("failed to stop the test Kubernetes API, error: %v", err)
		}
	}()

	// Create a context that can be explicitly canceled.
	ctx, cancel := context.WithCancel(context.TODO())

	dynamicWatcherReconciler, sourceChan := client.NewControllerRuntimeSource()

	// Create the dynamic watcher using the generated reconciler.
	dynamicWatcher, err := client.New(k8sConfig, dynamicWatcherReconciler, nil)
	if err != nil {
		panic(err)
	}

	// Start the dynamic watcher in a separate goroutine to not block the main goroutine.
	go func() {
		err := dynamicWatcher.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()

	// Wait until the dynamic watcher has started.
	<-dynamicWatcher.Started()

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

	// Trigger the controller-runtime Reconcile method about watcher when watched1 is updated.
	err = dynamicWatcher.AddOrUpdateWatcher(watcher, watched1)
	if err != nil {
		panic(err)
	}

	// Create a controller-runtime manager and register a simple controller.
	options := ctrl.Options{
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				"default": {},
			},
		},
		Scheme: scheme.Scheme,
		Metrics: server.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	}

	mgr, err := ctrl.NewManager(k8sConfig, options)
	if err != nil {
		panic(err)
	}

	// This controller watches ConfigMaps and will additionally reconcile any time the dynamic watcher sees a watched
	// object is updated.
	err = ctrl.NewControllerManagedBy(mgr).
		Named("ExampleNewControllerRuntimeSource").
		For(&corev1.ConfigMap{}).
		WatchesRawSource(sourceChan).
		Complete(&ctrlRuntimeReconciler{})
	if err != nil {
		panic(err)
	}

	// Simulate something canceling the context in 5 seconds so that the example exits.
	go func() {
		time.Sleep(5 * time.Second)

		cancel()
	}()

	err = mgr.Start(ctx)
	if err != nil {
		panic(err)
	}

	// Output:
}

func ExampleDynamicWatcher_Get() { //nolint: nosnakecase
	// Start a test Kubernetes API.
	testEnv := envtest.Environment{}

	k8sConfig, err := testEnv.Start()
	if err != nil {
		panic(err)
	}

	// Create two test secrets to watch and cache
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		panic(err)
	}

	namespace := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "example",
		},
	}

	_, err = k8sClient.CoreV1().Namespaces().Create(context.TODO(), &namespace, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}

	secret1 := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watched1",
			Namespace: "example",
		},
	}

	_, err = k8sClient.CoreV1().Secrets("example").Create(context.TODO(), &secret1, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}

	secret2 := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watched2",
			Namespace: "example",
		},
	}

	_, err = k8sClient.CoreV1().Secrets("example").Create(context.TODO(), &secret2, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}

	defer func() {
		err := testEnv.Stop()
		if err != nil {
			klog.Errorf("failed to stop the test Kubernetes API, error: %v", err)
		}
	}()

	// Create the dynamic watcher with the cache enabled.
	dynamicWatcher, err := client.New(
		k8sConfig, &reconciler{}, &client.Options{DisableInitialReconcile: true, EnableCache: true},
	)
	if err != nil {
		panic(err)
	}

	// Create a child context that can be explicitly canceled.
	ctx, cancel := context.WithCancel(context.Background())

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
		Namespace: "example",
		Name:      "watcher",
	}

	// Starting a query batch associates the get queries below as watched objects of this watcher.
	err = dynamicWatcher.StartQueryBatch(watcher)
	if err != nil {
		panic(err)
	}

	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Secret"}

	// This creates a watch on watched1 and caches the object.
	cachedSecret1, err := dynamicWatcher.Get(watcher, gvk, "example", "watched1")
	if err != nil {
		panic(err)
	}

	fmt.Println(cachedSecret1.GetName())

	// This creates a watch on watched1 and caches the object.
	cachedSecret2, err := dynamicWatcher.Get(watcher, gvk, "example", "watched2")
	if err != nil {
		panic(err)
	}

	fmt.Println(cachedSecret2.GetName())

	// Ending a query batch will clean up any previous watches not referenced in the batch. In this case, there were
	// none.
	err = dynamicWatcher.EndQueryBatch(watcher)
	if err != nil {
		panic(err)
	}

	// Retrieve directly from the cache
	cachedSecret1, err = dynamicWatcher.GetFromCache(gvk, "example", "watched1")
	if err != nil {
		panic(err)
	}

	fmt.Println(cachedSecret1.GetName())

	// Retrieve watched objects from the cache for the watcher
	cachedSecrets, err := dynamicWatcher.ListWatchedFromCache(watcher)
	if err != nil {
		panic(err)
	}

	// Sort the slice so the output is consistent for the output validation
	sort.Slice(cachedSecrets, func(i, j int) bool { return cachedSecrets[i].GetName() < cachedSecrets[j].GetName() })

	for _, cachedSecret := range cachedSecrets {
		fmt.Println(cachedSecret.GetName())
	}

	// Run until the context is canceled.
	<-ctx.Done()

	// Output:
	// watched1
	// watched2
	// watched1
	// watched1
	// watched2
}
