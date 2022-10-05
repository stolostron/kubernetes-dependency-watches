// Copyright Contributors to the Open Cluster Management project

package client

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type k8sObject interface {
	GroupVersionKind() schema.GroupVersionKind
	GetName() string
	GetNamespace() string
}

// toObjectIdentifer will convert a Kubernetes object from the client-go library to an ObjectIdentifier.
func toObjectIdentifer(object k8sObject) ObjectIdentifier {
	return ObjectIdentifier{
		Group:     object.GroupVersionKind().Group,
		Version:   object.GroupVersionKind().Version,
		Kind:      object.GroupVersionKind().Kind,
		Namespace: object.GetNamespace(),
		Name:      object.GetName(),
	}
}

type reconciler struct {
	ResultsChan chan ObjectIdentifier
}

// Reconcile just sends the input watcher to the r.ResultsChan channel so that tests can read the results
// to see if the appropriate number of reconciles were called.
func (r *reconciler) Reconcile(ctxTest context.Context, watcher ObjectIdentifier) (reconcile.Result, error) {
	r.ResultsChan <- watcher

	return reconcile.Result{}, nil
}

func getDynamicWatcher(ctx context.Context) (
	watcher *corev1.ConfigMap, watched []*corev1.Secret, reconcilerObj *reconciler, dynamicWatcher DynamicWatcher,
) {
	watcher = &corev1.ConfigMap{
		// This verbose definition is required for the GVK to be present on the object.
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watcher",
			Namespace: namespace,
		},
	}

	var err error
	_, err = k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, watcher, metav1.CreateOptions{})
	Expect(err).To(BeNil())

	for _, name := range []string{"watched1", "watched2"} {
		obj := &corev1.Secret{
			// This verbose definition is required for the GVK to be present on the object.
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Secret",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}

		_, err = k8sClient.CoreV1().Secrets(namespace).Create(ctx, obj, metav1.CreateOptions{})
		Expect(err).To(BeNil())

		watched = append(watched, obj)
	}

	reconcilerObj = &reconciler{
		ResultsChan: make(chan ObjectIdentifier, 20),
	}

	dynamicWatcher, err = New(k8sConfig, reconcilerObj, nil)
	Expect(err).To(BeNil())
	Expect(dynamicWatcher).NotTo(BeNil())

	// Return the named return values.
	return
}

var _ = Describe("Test the client", Ordered, func() {
	var (
		ctxTest        context.Context
		cancelCtxTest  context.CancelFunc
		dynamicWatcher DynamicWatcher
		reconcilerObj  *reconciler
		watched        []*corev1.Secret
		watchedObjIDs  []ObjectIdentifier
		watcher        *corev1.ConfigMap
	)

	BeforeAll(func() {
		ctxTest, cancelCtxTest = context.WithCancel(ctx)
		watcher, watched, reconcilerObj, dynamicWatcher = getDynamicWatcher(ctx)

		watchedObjIDs = []ObjectIdentifier{}
		for _, watchedObj := range watched {
			watchedObjIDs = append(watchedObjIDs, toObjectIdentifer(watchedObj))
		}

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).To(BeNil())
		}()

		<-dynamicWatcher.Started()
	})

	AfterEach(func() {
		// Drain the test results channel.
		for len(reconcilerObj.ResultsChan) != 0 {
			_, ok := <-reconcilerObj.ResultsChan
			Expect(ok).To(BeTrue())
		}
	})

	AfterAll(func() {
		for _, objectID := range watched {
			err := k8sClient.CoreV1().Secrets(namespace).Delete(ctxTest, objectID.Name, metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).To(BeNil())
			}
		}

		err := k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctxTest, watcher.Name, metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).To(BeNil())
		}

		cancelCtxTest()
	})

	It("Ensures watches are not added when invalid input is provided", func() {
		By("Verifying that an error is returned when no watched objects are provided")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher))
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Verifying that an error is returned when an invalid watcher object is provided")
		err = dynamicWatcher.AddOrUpdateWatcher(ObjectIdentifier{}, watchedObjIDs...)
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Verifying that an error is returned when an invalid watched object is provided")
		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), ObjectIdentifier{})
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Verifying that an error is returned when the watched object does not have a CRD installed")
		obj := ObjectIdentifier{
			Group:     "",
			Version:   "v1",
			Kind:      "SuperCoolConfigMap",
			Namespace: "test-ns",
			Name:      "watcher",
		}
		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs[0], obj)
		Expect(err).ToNot(BeNil())

		By("Verifying that the previous encountered error resulted in the watch being cleaned up")
		Eventually(dynamicWatcher.GetWatchCount, "3s").Should(Equal(uint(0)))
	})

	It("Adds watches", func() {
		By("Adding the watcher with a single watched object")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs[0])
		Expect(err).To(BeNil())

		By("Checking that the reconciler was called during the initial list")
		Eventually(reconcilerObj.ResultsChan, "5s").Should(HaveLen(1))

		By("Update the watcher with both watched objects")
		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs...)
		Expect(err).To(BeNil())

		By("Checking that the reconciler was called during the initial list")
		Eventually(reconcilerObj.ResultsChan, "5s").Should(HaveLen(2))

		By("Checking that the watcher object was passed to the reconciler")
		for len(reconcilerObj.ResultsChan) != 0 {
			objectID, ok := <-reconcilerObj.ResultsChan
			Expect(ok).To(BeTrue())
			Expect(objectID.Kind).To(Equal("ConfigMap"))
			Expect(objectID.Name).To(Equal("watcher"))
		}
	})

	It("Ensures the reconciler is called", func() {
		By("Updating a watched object")
		watched[0].Labels = map[string]string{"watch": "me"}
		_, err := k8sClient.CoreV1().Secrets(namespace).Update(ctxTest, watched[0], metav1.UpdateOptions{})
		Expect(err).To(BeNil())

		Eventually(reconcilerObj.ResultsChan, "5s").Should(HaveLen(1))
	})

	It("Verifies the watch count", func() {
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Ensures no duplicate watches are added", func() {
		By("Making a watched object be watched by another watcher")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watched[0]), toObjectIdentifer(watched[1]))
		Expect(err).To(BeNil())

		By("Checking the watch count")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Removes the watcher", func() {
		By("Verifying than an error is returned when watcher is invalid")
		err := dynamicWatcher.RemoveWatcher(ObjectIdentifier{})
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Removing the second watcher")
		err = dynamicWatcher.RemoveWatcher(toObjectIdentifer(watched[0]))
		Expect(err).To(BeNil())

		By("Checking that the watch wasn't garbage collected")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))

		By("Updating the first watcher to watch only one object")
		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), toObjectIdentifer(watched[0]))
		Expect(err).To(BeNil())

		By("Checking that a watch was garbage collected")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))

		By("Removing the first watcher entirely")
		err = dynamicWatcher.RemoveWatcher(toObjectIdentifer(watcher))
		Expect(err).To(BeNil())

		By("Checking that the remaining watch is garbage collected")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(0)))

		By("Checking that no reconcile happened from removing the watches")
		Expect(reconcilerObj.ResultsChan).Should(HaveLen(0))

		By("Updating a previously watched object to ensure no reconcile is called")
		watched[0].Labels = map[string]string{"watch": "me-too"}
		_, err = k8sClient.CoreV1().Secrets(namespace).Update(ctxTest, watched[0], metav1.UpdateOptions{})
		Expect(err).To(BeNil())

		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(0))
	})
})

var _ = Describe("Test the client clean up", Ordered, func() {
	var (
		ctxTest        context.Context
		cancelCtxTest  context.CancelFunc
		dynamicWatcher DynamicWatcher
		watched        []*corev1.Secret
		watcher        *corev1.ConfigMap
	)

	BeforeAll(func() {
		ctxTest, cancelCtxTest = context.WithCancel(ctx)
		watcher, watched, _, dynamicWatcher = getDynamicWatcher(ctx)

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).To(BeNil())
		}()

		<-dynamicWatcher.Started()
	})

	AfterAll(func() {
		for _, objectID := range watched {
			// Use the parent context since ctxTest is closed during the test.
			err := k8sClient.CoreV1().Secrets(namespace).Delete(ctx, objectID.Name, metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).To(BeNil())
			}
		}

		err := k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctx, watcher.Name, metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).To(BeNil())
		}
	})

	It("Verifies the client cleans up watches", func() {
		watchedObjIDs := []ObjectIdentifier{}
		for _, watchedObj := range watched {
			watchedObjIDs = append(watchedObjIDs, toObjectIdentifer(watchedObj))
		}

		By("Adding the watcher with two watched objects")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs...)
		Expect(err).To(BeNil())

		By("Verifying the watch count")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))

		By("Stopping the client")
		cancelCtxTest()

		By("Verifying the watches were garbage collected")
		Eventually(dynamicWatcher.GetWatchCount, "5s").Should(Equal(uint(0)))
	})
})

var _ = Describe("Test ObjectIdentifier", func() {
	It("Verifies the ObjectIdentifier has a nice string format", func() {
		obj := ObjectIdentifier{
			Group:     "",
			Version:   "v1",
			Kind:      "ConfigMap",
			Namespace: "test-ns",
			Name:      "watcher",
		}
		Expect(obj.String()).To(Equal("/v1, Kind=ConfigMap, Namespace=test-ns, Name=watcher"))
	})

	It("Verifies ObjectIdentifier.Validate", func() {
		obj := ObjectIdentifier{
			Group:     "",
			Version:   "v1",
			Kind:      "ConfigMap",
			Namespace: "test-ns",
			Name:      "watcher",
		}
		err := obj.Validate()
		Expect(err).To(BeNil())

		obj.Version = ""
		err = obj.Validate()
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())
		obj.Version = "v1"

		obj.Kind = ""
		err = obj.Validate()
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())
		obj.Kind = "ConfigMap"

		obj.Name = ""
		err = obj.Validate()
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())
	})
})

type reconciler2 struct{}

var reconcileRV func() (reconcile.Result, error)

// Reconcile just sends the input watcher to the r.ResultsChan channel so that tests can read the results
// to see if the appropriate number of reconciles were called.
func (r *reconciler2) Reconcile(ctxTest context.Context, watcher ObjectIdentifier) (reconcile.Result, error) {
	return reconcileRV()
}

var _ = Describe("Test dynamicWatcher.reconcileHandler", Serial, func() {
	obj := ObjectIdentifier{
		Group:     "",
		Version:   "v1",
		Kind:      "ConfigMap",
		Namespace: "test-ns",
		Name:      "watcher",
	}

	It("Verifies a requeue occurs when the reconciler returns an error", func() {
		reconcileCount := 0
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		By("Making the Reconcile method return an error only on the first call")
		reconcileRV = func() (reconcile.Result, error) {
			reconcileCount++

			if reconcileCount == 1 {
				return reconcile.Result{}, errors.New("some error that should requeue")
			}

			return reconcile.Result{}, nil
		}

		rateLimiter := workqueue.DefaultControllerRateLimiter()

		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewRateLimitingQueue(rateLimiter),
			Reconciler:  &reconciler2{},
		}

		By("Handling a single object")
		dynWatcher.Queue.Add(obj)

		go func() {
			for dynWatcher.processNextWorkItem(ctx) {
			}
		}()

		By("Verify that the reconciler was called twice")
		Eventually(func() int { return reconcileCount }, "2s").Should(Equal(2))
		Consistently(func() int { return reconcileCount }, "2s").Should(Equal(2))
	})

	It("Verifies a requeue occurs when the Reconcile method returns a RequeueAfter value", func() {
		reconcileCount := 0
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		By("Making the Reconcile method return a RequeueAfter on the first call")
		reconcileRV = func() (reconcile.Result, error) {
			reconcileCount++

			if reconcileCount == 1 {
				return reconcile.Result{RequeueAfter: time.Millisecond * 500}, nil
			}

			return reconcile.Result{}, nil
		}

		rateLimiter := workqueue.DefaultControllerRateLimiter()

		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewRateLimitingQueue(rateLimiter),
			Reconciler:  &reconciler2{},
		}

		By("Handling a single object")
		dynWatcher.Queue.Add(obj)

		go func() {
			for dynWatcher.processNextWorkItem(ctx) {
			}
		}()

		By("Verify that the reconciler was called twice")
		Eventually(func() int { return reconcileCount }, "2s").Should(Equal(2))
		Consistently(func() int { return reconcileCount }, "2s").Should(Equal(2))
	})

	It("Verifies a requeue occurs when the Reconcile method returns Requeue=true", func() {
		reconcileCount := 0
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		By("Making the Reconcile method return Requeue=true on the first call")
		reconcileRV = func() (reconcile.Result, error) {
			reconcileCount++

			if reconcileCount == 1 {
				return reconcile.Result{Requeue: true}, nil
			}

			return reconcile.Result{}, nil
		}

		rateLimiter := workqueue.DefaultControllerRateLimiter()

		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewRateLimitingQueue(rateLimiter),
			Reconciler:  &reconciler2{},
		}

		By("Handling a single object")
		dynWatcher.Queue.Add(obj)

		go func() {
			for dynWatcher.processNextWorkItem(ctx) {
			}
		}()

		By("Verify that the reconciler was called twice")
		Eventually(func() int { return reconcileCount }, "2s").Should(Equal(2))
		Consistently(func() int { return reconcileCount }, "2s").Should(Equal(2))
	})

	It("Ensures invalid input in the queue is dropped", func() {
		By("Making the Reconcile method keep track of the number of reconciles")
		reconcileCount := 0
		reconcileRV = func() (reconcile.Result, error) {
			reconcileCount++

			return reconcile.Result{}, nil
		}

		rateLimiter := workqueue.DefaultControllerRateLimiter()
		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewRateLimitingQueue(rateLimiter),
			Reconciler:  &reconciler2{},
		}

		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		go func() {
			for dynWatcher.processNextWorkItem(ctx) {
			}
		}()

		By("Handling an invalid object")
		dynWatcher.Queue.Add("Not an ObjectIdentifier object")

		By("Verifying the invalid object was dropped and the Reconcile method was not called")
		Eventually(dynWatcher.Queue.Len, "2s").Should(Equal(0))
		Consistently(func() int { return reconcileCount }, "2s").Should(Equal(0))
	})
})

var _ = Describe("Test dynamicWatcher not started", func() {
	watcher := ObjectIdentifier{
		Group:     "",
		Version:   "v1",
		Kind:      "ConfigMap",
		Namespace: "test-ns",
		Name:      "watcher",
	}
	watched := ObjectIdentifier{
		Group:     "",
		Version:   "v1",
		Kind:      "ConfigMap",
		Namespace: "test-ns",
		Name:      "watched",
	}

	It("Does not allow adding a watch when not started", func() {
		dynWatcher := dynamicWatcher{}
		err := dynWatcher.AddOrUpdateWatcher(watcher, watched)
		Expect(errors.Is(err, ErrNotStarted)).To(BeTrue())
	})

	It("Does not allow removing a watch when not started", func() {
		dynWatcher := dynamicWatcher{}
		err := dynWatcher.RemoveWatcher(watcher)
		Expect(errors.Is(err, ErrNotStarted)).To(BeTrue())
	})
})
