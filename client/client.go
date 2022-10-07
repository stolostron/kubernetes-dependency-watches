// Copyright Contributors to the Open Cluster Management project

// Package client is an event-driven Go library used when Kubernetes objects need to track when other objects change.
// The API is heavily based on the popular sigs.k8s.io/controller-runtime library.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"

	apimachinerymeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/ratelimiter"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	ErrNotStarted   error = errors.New("DynamicWatcher must be started to perform this action")
	ErrInvalidInput error = errors.New("invalid input provided")
)

type Reconciler interface {
	// Reconcile is called whenever an object has started being watched (if it exists) as well as when it has changed
	// (added, modified, or deleted). If an error is returned, the request is requeued.
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

	watcher := dynamicWatcher{
		client:            client,
		dynamicClient:     dynamicClient,
		rateLimiter:       rateLimiter,
		Reconciler:        reconciler,
		startedChan:       make(chan struct{}),
		watchedToWatchers: map[ObjectIdentifier]map[ObjectIdentifier]bool{},
		watcherToWatches:  map[ObjectIdentifier]map[ObjectIdentifier]bool{},
		watches:           map[ObjectIdentifier]watch.Interface{},
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
	// restMapper is used to map ObjectIdentifer objects to an API resource.
	restMapper apimachinerymeta.RESTMapper
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
	watches map[ObjectIdentifier]watch.Interface
}

// Start will start the dynamicWatcher and block until the input context is canceled.
func (d *dynamicWatcher) Start(ctx context.Context) error {
	klog.Info("Starting the dynamic watcher")

	d.Queue = workqueue.NewRateLimitingQueue(d.rateLimiter)

	err := d.refreshRESTMapper()
	if err != nil {
		return fmt.Errorf("failed to the Kubernetes API REST mapper: %w", err)
	}

	go func() {
		<-ctx.Done()

		klog.Info("Shutdown signal received, cleaning up the dynamic watcher")

		d.Queue.ShutDown()
	}()

	d.started = true
	close(d.startedChan)

	for d.processNextWorkItem(ctx) {
	}

	d.lock.Lock()

	klog.V(2).Infof("Cleaning up the %d leftover watches", len(d.watches))

	for _, watch := range d.watches {
		watch.Stop()
	}

	d.watchedToWatchers = map[ObjectIdentifier]map[ObjectIdentifier]bool{}
	d.watcherToWatches = map[ObjectIdentifier]map[ObjectIdentifier]bool{}
	d.watches = map[ObjectIdentifier]watch.Interface{}
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
// queue.
func (d *dynamicWatcher) relayWatchEvents(watchedObject ObjectIdentifier, events <-chan watch.Event) {
	for event := range events {
		// A watch error is usually from the watch request being explicitly stopped. This is why it's considered a
		// debug log.
		if event.Type == watch.Error {
			klog.V(2).Infof("An error was received from the watch request: %v", event.Object)

			continue
		}

		d.lock.RLock()

		for watcher := range d.watchedToWatchers[watchedObject] {
			d.Queue.Add(watcher)
		}

		d.lock.RUnlock()
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
		klog.Error(err, "Reconciler error")
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

// refreshRESTMapper will call the discovery API and create a RESTMapper that is used to convert a GVK to a GVR. An
// error is returned if the discovery API call failed.
func (d *dynamicWatcher) refreshRESTMapper() error {
	klog.V(2).Info("Refreshing the Kubernetes API REST Mapper")

	discovery := d.client.Discovery()

	apiGroups, err := restmapper.GetAPIGroupResources(discovery)
	if err != nil {
		klog.Error(err, "Could not get the API groups list from the Kubernetes API")

		return err
	}

	d.restMapper = restmapper.NewDiscoveryRESTMapper(apiGroups)

	return nil
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
		// If the object was previously watched, do nothing.
		if d.watcherToWatches[watcher][watchedObject] {
			watchedObjectsSet[watchedObject] = true

			continue
		}

		if len(d.watchedToWatchers[watchedObject]) == 0 {
			d.watchedToWatchers[watchedObject] = map[ObjectIdentifier]bool{}
		}

		// If the object is also watched another object, then do nothing.
		if _, ok := d.watches[watchedObject]; ok {
			watchedObjectsSet[watchedObject] = true
			d.watchedToWatchers[watchedObject][watcher] = true

			continue
		}

		gk := schema.GroupKind{Group: watchedObject.Group, Kind: watchedObject.Kind}

		mapping, err := d.restMapper.RESTMapping(gk, watchedObject.Version)
		if err != nil {
			// Refresh the REST mapper and try again if the mapping wasn't found.
			err = d.refreshRESTMapper()
			if err != nil {
				klog.Error(err, "failed to refresh the API discovery data")

				encounteredErr = err

				break
			}

			mapping, err = d.restMapper.RESTMapping(gk, watchedObject.Version)
			if err != nil {
				klog.Error(err, "Could not get resource mapping for %s", watchedObject)

				encounteredErr = err

				break
			}
		}

		var resource dynamic.ResourceInterface = d.dynamicClient.Resource(mapping.Resource)
		if watchedObject.Namespace != "" {
			resource = resource.(dynamic.NamespaceableResourceInterface).Namespace(watchedObject.Namespace)
		}

		watch, err := resource.Watch(
			context.TODO(), metav1.ListOptions{FieldSelector: "metadata.name=" + watchedObject.Name},
		)
		if err != nil {
			klog.Error(err, "Could not start a watch request for %s", watchedObject)

			encounteredErr = err

			break
		}

		d.watches[watchedObject] = watch
		d.watchedToWatchers[watchedObject][watcher] = true
		watchedObjectsSet[watchedObject] = true

		watchesAdded = append(watchesAdded, watchedObject)

		// Launch a Go routine per watch API request that will feed d.Queue.
		go d.relayWatchEvents(watchedObject, watch.ResultChan())
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

// removeWatch will remove a reference to the input watched object. If the references on the watched object become 0,
// the watch API request is stopped. Note that it's expected that the lock is already acquired by the caller.
func (d *dynamicWatcher) removeWatch(watcher ObjectIdentifier, watchedObject ObjectIdentifier) {
	delete(d.watcherToWatches[watcher], watchedObject)
	delete(d.watchedToWatchers[watchedObject], watcher)

	// Stop the watch API request if the watcher was the only one watching this object.
	if len(d.watchedToWatchers[watchedObject]) == 0 {
		d.watches[watchedObject].Stop()
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
