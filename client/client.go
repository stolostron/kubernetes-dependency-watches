// Copyright Contributors to the Open Cluster Management project

// Package client is an event-driven Go library used when Kubernetes objects need to track when other objects change.
// The API is heavily based on the popular sigs.k8s.io/controller-runtime library.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apiWatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/ratelimiter"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	ErrNotStarted          = errors.New("DynamicWatcher must be started to perform this action")
	ErrInvalidInput        = errors.New("invalid input provided")
	ErrNoVersionedResource = errors.New("the resource version was not found")
)

type Reconciler interface {
	// Reconcile is called whenever an object has started being watched (if it exists) as well as when it has changed
	// (added, modified, or deleted). If the watch stops prematurely and is restarted, it may cause a duplicate call to
	// this method. If an error is returned, the request is requeued.
	Reconcile(ctx context.Context, watcher ObjectIdentifier) (reconcile.Result, error)
}

// Options specify the arguments for creating a new DynamicWatcher.
type Options struct {
	// RateLimiter is used to limit how frequently requests may be queued.
	// Defaults to client-go's MaxOfRateLimiter which has both overall and per-item rate limiting. The overall is a
	// token bucket and the per-item is exponential.
	RateLimiter ratelimiter.RateLimiter
}

// ObjectIdentifier identifies an object from the Kubernetes API.
type ObjectIdentifier struct {
	Group     string
	Version   string
	Kind      string
	Namespace string
	Name      string
}

// String will convert the ObjectIdentifer to a string in a similar format to apimachinery's schema.GroupVersionKind.
func (o ObjectIdentifier) String() string {
	return o.Group + "/" + o.Version + ", Kind=" + o.Kind + ", Namespace=" + o.Namespace + ", Name=" + o.Name
}

// Validate will return a wrapped ErrInvalidInput error when a required field is not set on the ObjectIdentifier.
func (o ObjectIdentifier) Validate() error {
	if o.Version == "" {
		return fmt.Errorf("%w: the ObjectIdentifier (%s) Version must be set", ErrInvalidInput, o)
	}

	if o.Kind == "" {
		return fmt.Errorf("%w: the ObjectIdentifier (%s) Kind must be set", ErrInvalidInput, o)
	}

	if o.Name == "" {
		return fmt.Errorf("%w: the ObjectIdentifier (%s) Name must be set", ErrInvalidInput, o)
	}

	return nil
}

// DynamicWatcher implementations enable a consumer to be notified of updates to Kubernetes objects that other
// Kubernetes objects depend on.
type DynamicWatcher interface {
	// AddOrUpdateWatcher updates the watches for the watcher. When updating, any previously watched objects not
	// specified will stop being watched. If an error occurs, any created watches as part of this method execution will
	// be removed.
	AddOrUpdateWatcher(watcher ObjectIdentifier, watchedObjects ...ObjectIdentifier) error
	// RemoveWatcher removes a watcher and any of its API watches solely referenced by the watcher.
	RemoveWatcher(watcher ObjectIdentifier) error
	// Start will start the DynamicWatcher and block until the input context is canceled.
	Start(ctx context.Context) error
	// GetWatchCount returns the total number of active API watch requests which can be used for metrics.
	GetWatchCount() uint
	// Started returns a channel that is closed when the DynamicWatcher is ready to receive watch requests.
	Started() <-chan struct{}
}

// New returns an implemenetation of DynamicWatcher that is ready to be started with the Start method. An error is
// returned if Kubernetes clients can't be instantiated with the input Kubernetes configuration.
func New(config *rest.Config, reconciler Reconciler, options *Options) (DynamicWatcher, error) {
	if options == nil {
		options = &Options{}
	}

	rateLimiter := options.RateLimiter
	if rateLimiter == nil {
		rateLimiter = workqueue.DefaultControllerRateLimiter()
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize a Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize a dynamic Kubernetes client: %w", err)
	}

	gvkToGVRCache := ttlcache.New(
		ttlcache.WithTTL[schema.GroupVersionKind, schema.GroupVersionResource](10 * time.Minute),
	)
	// This will periodically delete expired cache items.
	go gvkToGVRCache.Start()

	watcher := dynamicWatcher{
		client:            client,
		dynamicClient:     dynamicClient,
		gvkToGVR:          gvkToGVRCache,
		rateLimiter:       rateLimiter,
		Reconciler:        reconciler,
		startedChan:       make(chan struct{}),
		watchedToWatchers: map[ObjectIdentifier]map[ObjectIdentifier]bool{},
		watcherToWatches:  map[ObjectIdentifier]map[ObjectIdentifier]bool{},
		watches:           map[ObjectIdentifier]apiWatch.Interface{},
	}

	return &watcher, nil
}

// dynamicWatcher implements the DynamicWatcher interface.
type dynamicWatcher struct {
	// dynamicClient is a client-go dynamic client used for the dynamic watches.
	dynamicClient dynamic.Interface
	// client is a client-go client used for API discovery.
	client kubernetes.Interface
	lock   sync.RWMutex
	Queue  workqueue.RateLimitingInterface
	Reconciler
	rateLimiter ratelimiter.RateLimiter
	// gvkToGVR is used as a cache of GVK to GVR mappings. The cache entries automatically expire every 10 minutes when
	// using the New function.
	gvkToGVR *ttlcache.Cache[schema.GroupVersionKind, schema.GroupVersionResource]
	// started gets set as part of the Start method.
	started bool
	// startedChan is closed when the dynamicWatcher is started. This is exposed to the user through the Started method.
	startedChan chan struct{}
	// watchedToWatchers is a map where the keys are ObjectIdentifier objects representing the watched objects.
	// Each value acts as a set of ObjectIdentifier objects representing the watcher objects.
	watchedToWatchers map[ObjectIdentifier]map[ObjectIdentifier]bool
	// watcherToWatches is a map where the keys are ObjectIdentifier objects representing the watcher objects.
	// Each value acts as a set of ObjectIdentifier objects representing the watched objects.
	watcherToWatches map[ObjectIdentifier]map[ObjectIdentifier]bool
	// watches is a map where the keys are ObjectIdentifier objects representing the watched objects and the values
	// are objects representing the Kubernetes watch API requests.
	watches map[ObjectIdentifier]apiWatch.Interface
}

// Start will start the dynamicWatcher and block until the input context is canceled.
func (d *dynamicWatcher) Start(ctx context.Context) error {
	klog.Info("Starting the dynamic watcher")

	d.Queue = workqueue.NewRateLimitingQueue(d.rateLimiter)

	go func() {
		<-ctx.Done()

		klog.Info("Shutdown signal received, cleaning up the dynamic watcher")

		d.Queue.ShutDown()
	}()

	d.started = true
	close(d.startedChan)

	//nolint: revive
	for d.processNextWorkItem(ctx) {
	}

	d.lock.Lock()

	klog.V(2).Infof("Cleaning up the %d leftover watches", len(d.watches))

	for _, watch := range d.watches {
		watch.Stop()
	}

	d.watchedToWatchers = map[ObjectIdentifier]map[ObjectIdentifier]bool{}
	d.watcherToWatches = map[ObjectIdentifier]map[ObjectIdentifier]bool{}
	d.watches = map[ObjectIdentifier]apiWatch.Interface{}
	d.startedChan = make(chan struct{})
	d.started = false

	d.lock.Unlock()

	return nil
}

// Started returns a channel that is closed when the dynamicWatcher is ready to receive watch requests.
func (d *dynamicWatcher) Started() <-chan struct{} {
	return d.startedChan
}

// relayWatchEvents will watch a channel tied to a Kubernetes API watch and then relay changes to the dynamicWatcher
// queue. If the watch stops unintentionally after retries from the client-go RetryWatcher, it will be restarted at the
// latest resourceVersion. This usually happens if the retry watcher tries to start watching again at a resource version
// that is no longer in etcd.
func (d *dynamicWatcher) relayWatchEvents(
	watchedObject ObjectIdentifier,
	resource dynamic.ResourceInterface,
	watchedObjectExists bool,
	events <-chan apiWatch.Event,
) {
	sendInitialEvent := watchedObjectExists

	for {
		// Send an initial event when the watch is started and the object exists to replicate the list and watch
		// behavior of controller-runtime. A watch restart can also trigger this to account for a lost event.
		if sendInitialEvent {
			d.lock.RLock()

			for watcher := range d.watchedToWatchers[watchedObject] {
				d.Queue.Add(watcher)
			}

			d.lock.RUnlock()

			sendInitialEvent = false
		}

		// This for loop exits when the events channel closes, thus signaling that the RetryWatch stopped.
		for event := range events {
			// A watch error is usually from the watch request being explicitly stopped. This is why it's considered a
			// debug log.
			if event.Type == apiWatch.Error {
				klog.V(2).Infof("An error was received from the watch request: %v", event.Object)

				continue
			}

			d.lock.RLock()

			for watcher := range d.watchedToWatchers[watchedObject] {
				d.Queue.Add(watcher)
			}

			d.lock.RUnlock()
		}

		// The RetryWatch stopped, so figure out if this was intentional or due to an error.
		d.lock.Lock()

		// If the dynamicWatcher is unaware of the watch, then that means the watch stopping is intentional and the
		// relay can end.
		if _, ok := d.watches[watchedObject]; !ok {
			klog.V(2).Infof("A watch channel for the watcher %s was closed", watchedObject)
			d.lock.Unlock()

			return
		}

		klog.V(1).Infof("Restarting the watch request for %s", watchedObject)

		w, _, err := watchLatest(watchedObject, resource)
		if err != nil {
			klog.Errorf("Could not restart a watch request for %s. Trying again. Error: %v", watchedObject, err)
			d.lock.Unlock()

			continue
		}

		d.watches[watchedObject] = w
		d.lock.Unlock()

		// Use the new watch channel which will be used in the next iteration of the loop.
		events = w.ResultChan()
		// Send the initial event in case an action was lost between watch requests.
		sendInitialEvent = true
	}
}

// processNextWorkItem will read a single work item off the queue and attempt to process it, by calling the
// reconcileHandler. A bool is returned based on if the queue is shutdown due to the dynamicWatcher shutting down.
func (d *dynamicWatcher) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := d.Queue.Get()
	if shutdown {
		// Tell the caller to stop calling this method once the queue has shutdown.
		return false
	}

	// Let the queue know that item has been handled after the Reconcile method is called.
	defer d.Queue.Done(obj)

	d.reconcileHandler(ctx, obj)

	return true
}

// reconcileHandler takes an object from the queue and calls the user's Reconcile method.
func (d *dynamicWatcher) reconcileHandler(ctx context.Context, obj interface{}) {
	// Verify that this is an actual ObjectIdentifier
	watcher, ok := obj.(ObjectIdentifier)
	if !ok {
		// Remove the invalid object from the queue.
		d.Queue.Forget(watcher)

		return
	}

	result, err := d.Reconcile(ctx, watcher)

	switch {
	case err != nil:
		d.Queue.AddRateLimited(watcher)
		klog.Errorf("Reconciler error: %v", err)
	case result.RequeueAfter > 0:
		// The result.RequeueAfter request will be lost, if it is returned
		// along with a non-nil error. But this is intended as
		// We need to drive to stable reconcile loops before queuing due
		// to result.RequestAfter
		d.Queue.Forget(watcher)
		d.Queue.AddAfter(watcher, result.RequeueAfter)
	case result.Requeue:
		d.Queue.AddRateLimited(watcher)
	default:
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		d.Queue.Forget(watcher)
	}
}

// AddOrUpdateWatcher updates the watches for the watcher. When updating, any previously watched objects not specified
// will stop being watched. If an error occurs, any created watches as part of this method execution will be removed.
func (d *dynamicWatcher) AddOrUpdateWatcher(watcher ObjectIdentifier, watchedObjects ...ObjectIdentifier) error {
	if !d.started {
		return ErrNotStarted
	}

	if len(watchedObjects) == 0 {
		return fmt.Errorf("%w: at least one watched object must be provided", ErrInvalidInput)
	}

	if err := watcher.Validate(); err != nil {
		return err
	}

	for _, watchedObject := range watchedObjects {
		if err := watchedObject.Validate(); err != nil {
			return err
		}
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	watchedObjectsSet := make(map[ObjectIdentifier]bool, len(watchedObjects))

	var encounteredErr error

	watchesAdded := []ObjectIdentifier{}

	for _, watchedObject := range watchedObjects {
		gvr, namespaced, err := d.gvrFromObjectIdentifier(watchedObject)
		if err != nil {
			klog.Errorf("Could not get the GVR for %s, error: %v", watchedObject, err)

			encounteredErr = err

			break
		}

		if !namespaced { // ignore namespaces set on cluster-scoped resources
			watchedObject.Namespace = ""
		}

		// If the object was previously watched, do nothing.
		if d.watcherToWatches[watcher][watchedObject] {
			watchedObjectsSet[watchedObject] = true

			continue
		}

		if len(d.watchedToWatchers[watchedObject]) == 0 {
			d.watchedToWatchers[watchedObject] = map[ObjectIdentifier]bool{}
		}

		// If the object is also watched by another object, then do nothing.
		if _, ok := d.watches[watchedObject]; ok {
			watchedObjectsSet[watchedObject] = true
			d.watchedToWatchers[watchedObject][watcher] = true

			continue
		}

		var resource dynamic.ResourceInterface = d.dynamicClient.Resource(gvr)
		if watchedObject.Namespace != "" {
			resource = resource.(dynamic.NamespaceableResourceInterface).Namespace(watchedObject.Namespace)
		}

		w, exists, err := watchLatest(watchedObject, resource)
		if err != nil {
			klog.Errorf("Could not start a watch request for %s, error: %v", watchedObject, err)

			encounteredErr = err

			break
		}

		d.watches[watchedObject] = w
		d.watchedToWatchers[watchedObject][watcher] = true
		watchedObjectsSet[watchedObject] = true

		watchesAdded = append(watchesAdded, watchedObject)

		// Launch a Go routine per watch API request that will feed d.Queue.
		go d.relayWatchEvents(watchedObject, resource, exists, w.ResultChan())
	}

	// If an error was encountered, remove the watches that were added to revert back to the previous state.
	if encounteredErr != nil {
		for _, addedWatch := range watchesAdded {
			d.removeWatch(watcher, addedWatch)
		}

		return encounteredErr
	}

	for existingWatchedObject := range d.watcherToWatches[watcher] {
		if watchedObjectsSet[existingWatchedObject] {
			continue
		}

		d.removeWatch(watcher, existingWatchedObject)
	}

	d.watcherToWatches[watcher] = watchedObjectsSet

	return nil
}

// watchLatest performs a list with the field selector for the input watched object to get the resource version and then
// starts the watch using the client-go RetryWatcher API. The returned bool indicates that the input watched object
// exists on the cluster.
func watchLatest(watchedObject ObjectIdentifier, resource dynamic.ResourceInterface) (apiWatch.Interface, bool, error) {
	fieldSelector := "metadata.name=" + watchedObject.Name
	timeout := int64(10)

	listResult, err := resource.List(
		context.TODO(), metav1.ListOptions{FieldSelector: fieldSelector, TimeoutSeconds: &timeout},
	)
	if err != nil {
		return nil, false, err
	}

	resourceVersion := listResult.GetResourceVersion()

	watchFunc := func(options metav1.ListOptions) (apiWatch.Interface, error) {
		options.FieldSelector = fieldSelector

		return resource.Watch(context.Background(), options)
	}

	w, err := watch.NewRetryWatcher(resourceVersion, &cache.ListWatch{WatchFunc: watchFunc})

	return w, len(listResult.Items) != 0, err
}

// removeWatch will remove a reference to the input watched object. If the references on the watched object become 0,
// the watch API request is stopped. Note that it's expected that the lock is already acquired by the caller.
func (d *dynamicWatcher) removeWatch(watcher ObjectIdentifier, watchedObject ObjectIdentifier) {
	delete(d.watcherToWatches[watcher], watchedObject)
	delete(d.watchedToWatchers[watchedObject], watcher)

	// Stop the watch API request if the watcher was the only one watching this object.
	if len(d.watchedToWatchers[watchedObject]) == 0 {
		// Defer this so that the map is cleared before the watch is stopped to signal to relayWatchEvents that this
		// is an intentional stop.
		defer d.watches[watchedObject].Stop()

		delete(d.watches, watchedObject)
		delete(d.watchedToWatchers, watchedObject)
	}
}

// RemoveWatcher removes a watcher and any of its API watches solely referenced by the watcher.
func (d *dynamicWatcher) RemoveWatcher(watcher ObjectIdentifier) error {
	if !d.started {
		return ErrNotStarted
	}

	if err := watcher.Validate(); err != nil {
		return err
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	for watchedObject := range d.watcherToWatches[watcher] {
		d.removeWatch(watcher, watchedObject)
	}

	delete(d.watcherToWatches, watcher)

	return nil
}

// GetWatchCount returns the total number of active API watch requests which can be used for metrics.
func (d *dynamicWatcher) GetWatchCount() uint {
	d.lock.RLock()

	count := uint(len(d.watches))

	d.lock.RUnlock()

	return count
}

// gvrFromObjectIdentifier uses the discovery client to get the versioned resource and whether it is
// namespaced. If the resource is not found or could not be retrieved, an error is always returned.
func (d *dynamicWatcher) gvrFromObjectIdentifier(watchedObject ObjectIdentifier) (
	schema.GroupVersionResource, bool, error,
) {
	gvk := schema.GroupVersionKind{
		Group: watchedObject.Group, Version: watchedObject.Version, Kind: watchedObject.Kind,
	}

	// First check the cache
	if cachedGVR := d.gvkToGVR.Get(gvk); cachedGVR != nil {
		// Delete the cached item if it's expired
		if cachedGVR.IsExpired() {
			d.gvkToGVR.Delete(gvk)
		} else {
			return cachedGVR.Value(), false, nil
		}
	}

	rsrcList, err := d.client.Discovery().ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return schema.GroupVersionResource{}, false, fmt.Errorf("%w: %s", ErrNoVersionedResource, gvk.String())
		}

		return schema.GroupVersionResource{}, false, err
	}

	for _, rsrc := range rsrcList.APIResources {
		if rsrc.Kind == gvk.Kind {
			gvr := schema.GroupVersionResource{
				Group:    gvk.Group,
				Version:  gvk.Version,
				Resource: rsrc.Name,
			}

			// Cache the value
			d.gvkToGVR.Set(gvk, gvr, ttlcache.DefaultTTL)

			return gvr, rsrc.Namespaced, nil
		}
	}

	return schema.GroupVersionResource{}, false, fmt.Errorf(
		"%w: no matching kind was found: %s", ErrNoVersionedResource, gvk.String(),
	)
}
