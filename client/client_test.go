// Copyright Contributors to the Open Cluster Management project

package client

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
func (r *reconciler) Reconcile(_ context.Context, watcher ObjectIdentifier) (reconcile.Result, error) {
	r.ResultsChan <- watcher

	return reconcile.Result{}, nil
}

func getDynamicWatcher(ctx context.Context, reconcilerObj Reconciler, options *Options) (
	watcher *corev1.ConfigMap, watched []k8sObject, dynamicWatcher DynamicWatcher,
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
	Expect(err).ToNot(HaveOccurred())

	DeferCleanup(func(dctx context.Context) {
		err = k8sClient.CoreV1().ConfigMaps(namespace).Delete(dctx, watcher.Name, metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}
	})

	watchedSecret := &corev1.Secret{
		// This verbose definition is required for the GVK to be present on the object.
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watched1",
			Namespace: namespace,
		},
	}

	_, err = k8sClient.CoreV1().Secrets(namespace).Create(ctx, watchedSecret, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())

	DeferCleanup(func(dctx context.Context) {
		err := k8sClient.CoreV1().Secrets(namespace).Delete(dctx, watchedSecret.Name, metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}
	})

	watched = append(watched, watchedSecret)

	watchedClusterRole := &rbacv1.ClusterRole{
		// This verbose definition is required for the GVK to be present on the object.
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "ClusterRole",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "watched2",
		},
	}

	_, err = k8sClient.RbacV1().ClusterRoles().Create(ctx, watchedClusterRole, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())

	DeferCleanup(func(dctx context.Context) {
		err = k8sClient.RbacV1().ClusterRoles().Delete(dctx, watchedClusterRole.Name, metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}
	})

	watched = append(watched, watchedClusterRole)

	dynamicWatcher, err = New(k8sConfig, reconcilerObj, options)
	Expect(err).ToNot(HaveOccurred())
	Expect(dynamicWatcher).NotTo(BeNil())

	// Return the named return values.
	return
}

var _ = Describe("Test a client without watch permissions", Ordered, func() {
	var (
		ctxTest       context.Context
		dynWatcher    DynamicWatcher
		reconcilerObj *reconciler
		watcher       *corev1.ConfigMap
		watched       *corev1.ConfigMap
	)

	BeforeAll(func() {
		var cancelCtxTest context.CancelFunc
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		DeferCleanup(func() { cancelCtxTest() })

		watcher = &corev1.ConfigMap{
			// This verbose definition is required for the GVK to be present on the object.
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "ConfigMap",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "watcher-test",
				Namespace: namespace,
			},
		}

		_, err := k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, watcher, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		DeferCleanup(func() {
			err := k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctx, watcher.Name, metav1.DeleteOptions{})
			Expect(err).ToNot(HaveOccurred())
		})

		// It really doesn't matter what the object being watched is so just reuse the watcher
		watched = watcher

		reconcilerObj = &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}

		kubeconfig := noWatchUser.Config()
		// Required for tests that involve restarting the test environment since new certs are generated.
		kubeconfig.TLSClientConfig.Insecure = true
		kubeconfig.TLSClientConfig.CAData = nil

		dynWatcher, err = New(kubeconfig, reconcilerObj, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(dynWatcher).NotTo(BeNil())

		go func() {
			defer GinkgoRecover()

			err := dynWatcher.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
		}()

		<-dynWatcher.Started()
	})

	AfterAll(func(ctx SpecContext) {
		err := k8sClient.RbacV1().ClusterRoleBindings().Delete(ctx, "watch-all", metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}

		err = k8sClient.RbacV1().ClusterRoles().Delete(ctx, "watch-all", metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}
	})

	AfterEach(func() {
		// Drain the test results channel.
		for len(reconcilerObj.ResultsChan) != 0 {
			_, ok := <-reconcilerObj.ResultsChan
			Expect(ok).To(BeTrue())
		}
	})

	It("Ensures the watch fails if the user is unauthorized", func() {
		err := dynWatcher.AddWatcher(toObjectIdentifer(watcher), toObjectIdentifer(watched))
		Expect(k8serrors.IsForbidden(err)).To(BeTrue(), "Expected the error to be forbidden")
	})

	It("Grant access and try again", func() {
		By("Granting the user watch access")
		watchAllRole := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "watch-all"},
			Rules: []rbacv1.PolicyRule{
				{
					Verbs:     []string{"watch"},
					Resources: []string{"*"},
					APIGroups: []string{"*"},
				},
			},
		}

		_, err := k8sClient.RbacV1().ClusterRoles().Create(ctx, watchAllRole, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		watchAllBinding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "watch-all"},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     watchAllRole.Name,
			},
			Subjects: []rbacv1.Subject{
				{
					APIGroup: "rbac.authorization.k8s.io",
					Kind:     "User",
					Name:     "no-watch",
				},
			},
		}

		_, err = k8sClient.RbacV1().ClusterRoleBindings().Create(ctx, watchAllBinding, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("Adding the watch")
		err = dynWatcher.AddWatcher(toObjectIdentifer(watcher), toObjectIdentifer(watched))
		Expect(err).ToNot(HaveOccurred())
		Eventually(dynWatcher.GetWatchCount, "3s").Should(Equal(uint(1)))
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(1))
	})

	It("Ensures the watches restart on a Kubernetes API outage reevaluate permissions", func() {
		By("Removing the user watch access")
		err := k8sClient.RbacV1().ClusterRoleBindings().Delete(ctx, "watch-all", metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("Stopping the RetryWatcher under the hood to simulate an unrecoverable error")
		typedDynamicWatcher, ok := dynWatcher.(*dynamicWatcher)
		Expect(ok).To(BeTrue(), "Expected the DynamicWatcher interface to be dynamicWatcher type")
		typedDynamicWatcher.lock.Lock()
		typedDynamicWatcher.watches[toObjectIdentifer(watched)].watch.Stop()
		typedDynamicWatcher.lock.Unlock()

		By("Checking the watch is removed")
		Eventually(dynWatcher.GetWatchCount, "10s").Should(Equal(uint(0)))

		By("Checking that a reconcile for the object was called after the watch was removed")
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(1))
	})
})

var _ = Describe("Test the client", Ordered, Serial, func() {
	var (
		ctxTest        context.Context
		dynamicWatcher DynamicWatcher
		reconcilerObj  *reconciler
		watched        []k8sObject
		watchedObjIDs  []ObjectIdentifier
		watcher        *corev1.ConfigMap
	)

	BeforeAll(func() {
		var cancelCtxTest context.CancelFunc
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		DeferCleanup(func() { cancelCtxTest() })

		reconcilerObj = &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}
		watcher, watched, dynamicWatcher = getDynamicWatcher(ctxTest, reconcilerObj, nil)

		watchedObjIDs = []ObjectIdentifier{}
		for _, watchedObj := range watched {
			id := toObjectIdentifer(watchedObj)
			id.Namespace = namespace // ensure namespace is set, even (possibly "incorrectly") on cluster-scoped objects
			watchedObjIDs = append(watchedObjIDs, id)
		}

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
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

	It("Ensures watches are not added when invalid input is provided", func() {
		By("Verifying that an error is returned when no watched objects are provided")
		err := dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), ObjectIdentifier{})
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		err = dynamicWatcher.AddOrUpdateWatcher(ObjectIdentifier{}, watchedObjIDs...)
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Verifying that an error is returned when an invalid watcher object is provided")
		err = dynamicWatcher.AddWatcher(ObjectIdentifier{}, watchedObjIDs[0])
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		err = dynamicWatcher.AddOrUpdateWatcher(ObjectIdentifier{}, watchedObjIDs...)
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Verifying that an error is returned when an invalid watched object is provided")
		err = dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), ObjectIdentifier{})
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

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
		err = dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), obj)
		Expect(err).To(HaveOccurred())

		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs[0], obj)
		Expect(err).To(HaveOccurred())

		By("Verifying that the previous encountered error resulted in the watch being cleaned up")
		Eventually(dynamicWatcher.GetWatchCount, "3s").Should(Equal(uint(0)))

		By("Verifying that the existing watchers are retained when the watched object does not have a CRD installed")
		err = dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), toObjectIdentifer(watched[0]))
		Expect(err).ToNot(HaveOccurred())
		Eventually(dynamicWatcher.GetWatchCount, "3s").Should(Equal(uint(1)))

		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), obj)
		Expect(err).To(HaveOccurred())

		err = dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), obj)
		Expect(err).To(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).Should(Equal(uint(1)))

		// Clean up the watch
		err = dynamicWatcher.RemoveWatcher(toObjectIdentifer(watcher))
		Expect(err).ToNot(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).Should(Equal(uint(0)))
	})

	It("Adds watches", func() {
		By("Adding the watcher with a single watched object")
		err := dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), watchedObjIDs[0])
		Expect(err).ToNot(HaveOccurred())

		By("Checking that the reconciler was called during the initial list")
		Eventually(reconcilerObj.ResultsChan, "5s").Should(HaveLen(1))

		By("Update the watcher with both watched objects")
		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs...)
		Expect(err).ToNot(HaveOccurred())

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
		watchedSecret := watched[0].(*corev1.Secret)
		watchedSecret.Labels = map[string]string{"watch": "me"}
		_, err := k8sClient.CoreV1().Secrets(namespace).Update(ctxTest, watchedSecret, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(reconcilerObj.ResultsChan, "5s").Should(HaveLen(1))
	})

	It("Ensures the watches restart on a Kubernetes API outage", func() {
		By("Restarting the Kubernetes API server")
		Expect(testEnv.ControlPlane.APIServer.Stop()).To(Succeed())
		Expect(testEnv.ControlPlane.APIServer.Start()).To(Succeed())

		By("Checking that a reconcile for each object was called")
		// This is a longer timeout to account for the API server needing to start up.
		Eventually(reconcilerObj.ResultsChan, "60s").Should(HaveLen(2))
	})

	It("Verifies the watch count", func() {
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Ensures no duplicate watches are added", func() {
		By("Making a watched object be watched by another watcher")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watched[0]), toObjectIdentifer(watched[1]))
		Expect(err).ToNot(HaveOccurred())

		By("Checking the watch count")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Removes the watcher", func() {
		By("Verifying that an error is returned when watcher is invalid")
		err := dynamicWatcher.RemoveWatcher(ObjectIdentifier{})
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		By("Removing the second watcher")
		err = dynamicWatcher.RemoveWatcher(toObjectIdentifer(watched[0]))
		Expect(err).ToNot(HaveOccurred())

		By("Checking that the watch wasn't garbage collected")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))

		By("Updating the first watcher to watch only one object")
		err = dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), toObjectIdentifer(watched[0]))
		Expect(err).ToNot(HaveOccurred())

		By("Checking that a watch was garbage collected")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))

		By("Removing the first watcher entirely")
		err = dynamicWatcher.RemoveWatcher(toObjectIdentifer(watcher))
		Expect(err).ToNot(HaveOccurred())

		By("Checking that the remaining watch is garbage collected")
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(0)))

		By("Checking that no reconcile happened from removing the watches")
		Expect(reconcilerObj.ResultsChan).Should(BeEmpty())

		By("Updating a previously watched object to ensure no reconcile is called")
		watchedSecret := watched[0].(*corev1.Secret)
		watchedSecret.Labels = map[string]string{"watch": "me-too"}
		_, err = k8sClient.CoreV1().Secrets(namespace).Update(ctxTest, watchedSecret, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(0))
	})
})

var _ = Describe("Test the client with the initial reconcile is disabled", Ordered, func() {
	var (
		ctxTest        context.Context
		dynamicWatcher DynamicWatcher
		reconcilerObj  *reconciler
		watched        []k8sObject
		watchedObjIDs  []ObjectIdentifier
		watcher        *corev1.ConfigMap
	)

	BeforeAll(func() {
		var cancelCtxTest context.CancelFunc
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		DeferCleanup(func() { cancelCtxTest() })

		reconcilerObj = &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}
		watcher, watched, dynamicWatcher = getDynamicWatcher(
			ctxTest, reconcilerObj, &Options{DisableInitialReconcile: true},
		)

		watchedObjIDs = []ObjectIdentifier{}
		for _, watchedObj := range watched {
			id := toObjectIdentifer(watchedObj)
			id.Namespace = namespace // ensure namespace is set, even (possibly "incorrectly") on cluster-scoped objects
			watchedObjIDs = append(watchedObjIDs, id)
		}

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
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

	It("Adds watches", func() {
		By("Adding the watcher with a single watched object")
		err := dynamicWatcher.AddWatcher(toObjectIdentifer(watcher), watchedObjIDs[0])
		Expect(err).ToNot(HaveOccurred())

		By("Checking that the reconciler was called during the initial list")
		Consistently(reconcilerObj.ResultsChan, "5s").Should(HaveLen(0))

		By("Updating the watched object")
		watchedSecret := watched[0].(*corev1.Secret)
		watchedSecret.Labels = map[string]string{"trigger": "reconcile"}
		_, err = k8sClient.CoreV1().Secrets(namespace).Update(ctxTest, watchedSecret, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("Checking that the reconciler was called during the initial list")
		Eventually(reconcilerObj.ResultsChan, "5s").Should(HaveLen(1))
	})
})

var _ = Describe("Test the client clean up", Ordered, func() {
	var (
		watcherID      ObjectIdentifier
		dynamicWatcher DynamicWatcher
		reconcilerObj  *reconciler
	)

	BeforeAll(func() {
		testCtx, testCtxCancel := context.WithCancel(ctx)

		DeferCleanup(func() { testCtxCancel() })

		reconcilerObj = &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}

		var watcher *corev1.ConfigMap
		watcher, _, dynamicWatcher = getDynamicWatcher(testCtx, reconcilerObj, nil)
		watcherID = toObjectIdentifer(watcher)

		go func() {
			defer GinkgoRecover()

			Expect(dynamicWatcher.Start(testCtx)).To(Succeed())
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

	It("Adds the watcher with a selector", func() {
		watchedSelector := ObjectIdentifier{
			Group:     "",
			Version:   "v1",
			Kind:      "ConfigMap",
			Namespace: namespace,
			Selector:  "test-label=foo",
		}

		Expect(dynamicWatcher.AddOrUpdateWatcher(watcherID, watchedSelector)).To(Succeed())

		By("Checking there are no reconciles, since no matching objects exist")
		Consistently(reconcilerObj.ResultsChan, "3s").Should(BeEmpty())
	})

	It("Reconciles when a matching object is created or deleted", func() {
		cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:      "one",
			Namespace: namespace,
			Labels: map[string]string{
				"test-label": "foo",
			},
		}}

		_, err := k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, &cm, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		DeferCleanup(func(dctx context.Context) {
			err = k8sClient.CoreV1().ConfigMaps(namespace).Delete(dctx, cm.Name, metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		})

		By("Checking there was a reconcile")
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(1))
		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(1))

		By("Deleting the object")
		Expect(k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{})).To(Succeed())

		By("Checking there was a reconcile")
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(2))
		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(2))
	})

	It("Reconciles when an object is labeled to match the selector", func() {
		cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:      "two",
			Namespace: namespace,
			Labels: map[string]string{
				"test-label": "bar",
			},
		}}

		_, err := k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, &cm, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		DeferCleanup(func(dctx context.Context) {
			err = k8sClient.CoreV1().ConfigMaps(namespace).Delete(dctx, cm.Name, metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		})

		By("Checking there was not a reconcile because the object doesn't match")
		Consistently(reconcilerObj.ResultsChan, "3s").Should(BeEmpty())

		By("Updating the label to make the label match the selector")
		cm.Labels = map[string]string{"test-label": "foo"}
		_, err = k8sClient.CoreV1().ConfigMaps(namespace).Update(ctx, &cm, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("Checking there was a reconcile")
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(1))
		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(1))

		By("Updating the label to make the label not match the selector")
		cm.Labels = map[string]string{"test-label": "baz"}
		_, err = k8sClient.CoreV1().ConfigMaps(namespace).Update(ctx, &cm, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("Checking there was a reconcile")
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(2))
		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(2))

		By("Deleting the object")
		Expect(k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{})).Should(Succeed())

		By("Checking there was not an additional reconcile")
		Consistently(reconcilerObj.ResultsChan, "3s").Should(HaveLen(2))
	})
})

var _ = Describe("Test the client clean up", Ordered, func() {
	var (
		ctxTest        context.Context
		cancelCtxTest  context.CancelFunc
		dynamicWatcher DynamicWatcher
		watched        []k8sObject
		watcher        *corev1.ConfigMap
	)

	BeforeAll(func() {
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		DeferCleanup(func() {
			// The test should cancel the context, but this ensures it is cancelled during a failure
			cancelCtxTest()
		})

		reconcilerObj := &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}
		watcher, watched, dynamicWatcher = getDynamicWatcher(ctxTest, reconcilerObj, nil)

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
		}()

		<-dynamicWatcher.Started()
	})

	It("Verifies the client cleans up watches", func() {
		watchedObjIDs := []ObjectIdentifier{}
		for _, watchedObj := range watched {
			id := toObjectIdentifer(watchedObj)
			id.Namespace = namespace // ensure namespace is set, even (possibly "incorrectly") on cluster-scoped objects
			watchedObjIDs = append(watchedObjIDs, id)
		}

		By("Adding the watcher with two watched objects")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs...)
		Expect(err).ToNot(HaveOccurred())

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
		Expect(obj.String()).To(Equal("GroupVersion=v1, Kind=ConfigMap, Namespace=test-ns, Name=watcher"))
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
		Expect(err).ToNot(HaveOccurred())

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

		obj.Selector = "example.test/label=foo"
		Expect(obj.Validate()).To(Succeed())

		obj.Name = "watcher"
		err = obj.Validate()
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())

		obj.Name = ""
		obj.Selector = "something-random!="
		err = obj.Validate()
		Expect(errors.Is(err, ErrInvalidInput)).To(BeTrue())
	})
})

type reconciler2 struct{}

var reconcileRV func() (reconcile.Result, error)

// Reconcile just sends the input watcher to the r.ResultsChan channel so that tests can read the results
// to see if the appropriate number of reconciles were called.
func (r *reconciler2) Reconcile(_ context.Context, _ ObjectIdentifier) (reconcile.Result, error) {
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

		rateLimiter := workqueue.DefaultTypedControllerRateLimiter[ObjectIdentifier]()

		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewTypedRateLimitingQueue[ObjectIdentifier](rateLimiter),
			Reconciler:  &reconciler2{},
		}

		By("Handling a single object")
		dynWatcher.Queue.Add(obj)

		go func() {
			//nolint: revive
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

		rateLimiter := workqueue.DefaultTypedControllerRateLimiter[ObjectIdentifier]()

		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewTypedRateLimitingQueue[ObjectIdentifier](rateLimiter),
			Reconciler:  &reconciler2{},
		}

		By("Handling a single object")
		dynWatcher.Queue.Add(obj)

		go func() {
			//nolint: revive
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

		rateLimiter := workqueue.DefaultTypedControllerRateLimiter[ObjectIdentifier]()

		dynWatcher := dynamicWatcher{
			rateLimiter: rateLimiter,
			Queue:       workqueue.NewTypedRateLimitingQueue[ObjectIdentifier](rateLimiter),
			Reconciler:  &reconciler2{},
		}

		By("Handling a single object")
		dynWatcher.Queue.Add(obj)

		go func() {
			//nolint: revive
			for dynWatcher.processNextWorkItem(ctx) {
			}
		}()

		By("Verify that the reconciler was called twice")
		Eventually(func() int { return reconcileCount }, "2s").Should(Equal(2))
		Consistently(func() int { return reconcileCount }, "2s").Should(Equal(2))
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
		err := dynWatcher.AddWatcher(watcher, watched)
		Expect(err).To(MatchError(ErrNotStarted))
	})

	It("Does not allow updating a watch when not started", func() {
		dynWatcher := dynamicWatcher{}
		err := dynWatcher.AddOrUpdateWatcher(watcher, watched)
		Expect(err).To(MatchError(ErrNotStarted))
	})

	It("Does not allow removing a watch when not started", func() {
		dynWatcher := dynamicWatcher{}
		err := dynWatcher.RemoveWatcher(watcher)
		Expect(err).To(MatchError(ErrNotStarted))
	})

	It("Does not allow getting an object when not started", func() {
		dynWatcher := dynamicWatcher{}
		_, err := dynWatcher.Get(watcher, watched.GroupVersionKind(), watched.Namespace, watched.Name)
		Expect(err).To(MatchError(ErrNotStarted))
	})

	It("Does not allow listing an object when not started", func() {
		dynWatcher := dynamicWatcher{}
		_, err := dynWatcher.List(watcher, watched.GroupVersionKind(), "a-namespace", nil)
		Expect(err).To(MatchError(ErrNotStarted))
	})

	It("Does not allow starting a query batch when not started", func() {
		dynWatcher := dynamicWatcher{}
		err := dynWatcher.StartQueryBatch(watcher)
		Expect(err).To(MatchError(ErrNotStarted))
	})

	It("Does not allow stopping a query batch when not started", func() {
		dynWatcher := dynamicWatcher{}
		err := dynWatcher.EndQueryBatch(watcher)
		Expect(err).To(MatchError(ErrNotStarted))
	})
})

var _ = Describe("Test dynamicWatcher cache disabled errors", Ordered, func() {
	var (
		ctxTest        context.Context
		dynamicWatcher DynamicWatcher
		reconcilerObj  *reconciler
		watched        []k8sObject
		watcher        *corev1.ConfigMap
		watcherID      ObjectIdentifier
	)

	BeforeAll(func() {
		var cancelCtxTest context.CancelFunc
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		DeferCleanup(func() { cancelCtxTest() })

		reconcilerObj = &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}
		watcher, watched, dynamicWatcher = getDynamicWatcher(ctxTest, reconcilerObj, nil)
		watcherID = toObjectIdentifer(watcher)

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
		}()

		<-dynamicWatcher.Started()
	})

	It("Does not allow getting an object when disabled", func() {
		_, err := dynamicWatcher.Get(
			watcherID, watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)
		Expect(err).To(MatchError(ErrCacheDisabled))

		_, err = dynamicWatcher.GetFromCache(
			watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)
		Expect(err).To(MatchError(ErrCacheDisabled))
	})

	It("Does not allow listing objects when disabled", func() {
		_, err := dynamicWatcher.List(watcherID, watched[0].GroupVersionKind(), "a-namespace", nil)
		Expect(err).To(MatchError(ErrCacheDisabled))

		_, err = dynamicWatcher.ListFromCache(watched[0].GroupVersionKind(), "a-namespace", nil)
		Expect(err).To(MatchError(ErrCacheDisabled))

		_, err = dynamicWatcher.ListWatchedFromCache(watcherID)
		Expect(err).To(MatchError(ErrCacheDisabled))
	})

	It("Does not allow starting a query batch when disabled", func() {
		err := dynamicWatcher.StartQueryBatch(watcherID)
		Expect(err).To(MatchError(ErrCacheDisabled))
	})

	It("Does not allow stopping a query batch when disabled", func() {
		err := dynamicWatcher.EndQueryBatch(watcherID)
		Expect(err).To(MatchError(ErrCacheDisabled))
	})
})

var _ = Describe("Test the client query API", Ordered, func() {
	var (
		ctxTest        context.Context
		dynamicWatcher DynamicWatcher
		reconcilerObj  *reconciler
		watched        []k8sObject
		watcher        *corev1.ConfigMap
		watcherID      ObjectIdentifier
	)

	BeforeAll(func() {
		var cancelCtxTest context.CancelFunc
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		DeferCleanup(func() { cancelCtxTest() })

		reconcilerObj = &reconciler{
			ResultsChan: make(chan ObjectIdentifier, 20),
		}
		watcher, watched, dynamicWatcher = getDynamicWatcher(ctxTest, reconcilerObj, &Options{EnableCache: true})
		watcherID = toObjectIdentifer(watcher)

		go func() {
			defer GinkgoRecover()

			err := dynamicWatcher.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
		}()

		<-dynamicWatcher.Started()
	})

	AfterAll(func() {
		for _, name := range []string{"random-watched", "configmap-for-list1", "configmap-for-list2"} {
			err := k8sClient.CoreV1().ConfigMaps(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		}

		defaultNS, err := k8sClient.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		annotations := defaultNS.GetAnnotations()
		if annotations["my-test"] == "" {
			return
		}

		delete(annotations, "my-test")

		defaultNS.SetAnnotations(annotations)
		_, err = k8sClient.CoreV1().Namespaces().Update(ctx, defaultNS, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		// Drain the test results channel.
		for len(reconcilerObj.ResultsChan) != 0 {
			_, ok := <-reconcilerObj.ResultsChan
			Expect(ok).To(BeTrue())
		}
	})

	It("Does not allow Get calls when the batch isn't started", func() {
		_, err := dynamicWatcher.Get(
			watcherID, watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)

		Expect(err).To(MatchError(ErrQueryBatchNotStarted))
	})

	It("Does not allow List calls when the batch isn't started", func() {
		_, err := dynamicWatcher.List(watcherID, watched[0].GroupVersionKind(), "", nil)

		Expect(err).To(MatchError(ErrQueryBatchNotStarted))
	})

	It("Does not allow to end a query batch when the batch isn't started", func() {
		err := dynamicWatcher.EndQueryBatch(watcherID)

		Expect(err).To(MatchError(ErrQueryBatchNotStarted))
	})

	It("Allows directly adding watches before a query batch is started", func() {
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(0)))

		cm := corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ConfigMap",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "random-watched",
				Namespace: namespace,
			},
		}
		_, err := k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, &cm, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		err = dynamicWatcher.AddOrUpdateWatcher(watcherID, toObjectIdentifer(&cm))
		Expect(err).ToNot(HaveOccurred())

		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))
	})

	It("Utilizies the Get query API", func() {
		Expect(dynamicWatcher.StartQueryBatch(watcherID)).ToNot(HaveOccurred())

		cachedObj1, err := dynamicWatcher.Get(
			watcherID, watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)
		Expect(err).ToNot(HaveOccurred())

		Expect(cachedObj1.GetManagedFields()).To(BeEmpty())
		Expect(cachedObj1.GetAnnotations()["kubectl.kubernetes.io/last-applied-configuration"]).To(BeEmpty())
		Expect(cachedObj1.GroupVersionKind()).To(Equal(watched[0].GroupVersionKind()))
		Expect(cachedObj1.GetNamespace()).To(Equal(watched[0].GetNamespace()))
		Expect(cachedObj1.GetName()).To(Equal(watched[0].GetName()))
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("A duplicate Get query does not cause another watch", func() {
		cachedObj1, err := dynamicWatcher.Get(
			watcherID, watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)
		Expect(err).ToNot(HaveOccurred())

		Expect(cachedObj1.GroupVersionKind()).To(Equal(watched[0].GroupVersionKind()))
		Expect(cachedObj1.GetNamespace()).To(Equal(watched[0].GetNamespace()))
		Expect(cachedObj1.GetName()).To(Equal(watched[0].GetName()))
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Does not allow to start a query batch when the batch is already started", func() {
		err := dynamicWatcher.StartQueryBatch(watcherID)

		Expect(err).To(MatchError(ErrQueryBatchInProgress))
	})

	It("Gets an updated cache entry from an update", func() {
		cm, err := k8sClient.CoreV1().ConfigMaps(namespace).Get(ctx, "random-watched", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		cm.Annotations = map[string]string{"sometimes-i-feel-like": "somebody's-watching-me"}
		_, err = k8sClient.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		configmapGVK := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}

		Eventually(func(g Gomega) {
			cachedObject, err := dynamicWatcher.GetFromCache(configmapGVK, namespace, cm.Name)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cachedObject).ToNot(BeNil())
			g.Expect(cachedObject.GetAnnotations()["sometimes-i-feel-like"]).To(Equal("somebody's-watching-me"))
		}, "5s").Should(Succeed())
	})

	It("Gets an updated cache entry from a delete", func() {
		err := k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctx, "random-watched", metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())

		configmapGVK := schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}

		Eventually(func(g Gomega) {
			cachedObject, err := dynamicWatcher.GetFromCache(configmapGVK, namespace, "random-watched")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cachedObject).To(BeNil())
		}, "5s").Should(Succeed())
	})

	It("Returns the cached objects for the watcher", func() {
		cachedWatches, err := dynamicWatcher.ListWatchedFromCache(watcherID)

		Expect(err).ToNot(HaveOccurred())
		Expect(cachedWatches).To(HaveLen(1))
		Expect(cachedWatches[0].GroupVersionKind()).To(Equal(watched[0].GroupVersionKind()))
		Expect(cachedWatches[0].GetNamespace()).To(Equal(watched[0].GetNamespace()))
		Expect(cachedWatches[0].GetName()).To(Equal(watched[0].GetName()))
	})

	It("Returns the cached object for a watch", func() {
		cachedWatch, err := dynamicWatcher.GetFromCache(
			watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(cachedWatch.GroupVersionKind()).To(Equal(watched[0].GroupVersionKind()))
		Expect(cachedWatch.GetNamespace()).To(Equal(watched[0].GetNamespace()))
		Expect(cachedWatch.GetName()).To(Equal(watched[0].GetName()))
	})

	It("Allows a non-conflicting List query batch", func() {
		watcher := toObjectIdentifer(watched[1])

		cm := corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ConfigMap",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "configmap-for-list",
				Namespace: namespace,
				Labels: map[string]string{
					"watch": "me",
				},
			},
		}

		_, err := k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, &cm, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		DeferCleanup(func(dctx context.Context) {
			err = k8sClient.CoreV1().ConfigMaps(namespace).Delete(dctx, cm.Name, metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		})

		Expect(dynamicWatcher.StartQueryBatch(watcher)).ToNot(HaveOccurred())

		lsReq, err := labels.ParseToRequirements("watch=me")
		Expect(err).ToNot(HaveOccurred())
		ls := labels.NewSelector().Add(lsReq...)

		cachedObjects, err := dynamicWatcher.List(watcher, cm.GroupVersionKind(), namespace, ls)
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObjects).To(HaveLen(1))
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(3)))
		Expect(cachedObjects[0].GroupVersionKind()).To(Equal(cm.GroupVersionKind()))
		Expect(cachedObjects[0].GetNamespace()).To(Equal(cm.GetNamespace()))
		Expect(cachedObjects[0].GetName()).To(Equal(cm.GetName()))
	})

	It("Updates the cache when the list query watch updates", func() {
		watcher := toObjectIdentifer(watched[1])
		cm2 := corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ConfigMap",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "configmap-for-list2",
				Namespace: namespace,
				Labels: map[string]string{
					"watch": "me",
				},
			},
		}

		lsReq, err := labels.ParseToRequirements("watch=me")
		Expect(err).ToNot(HaveOccurred())
		ls := labels.NewSelector().Add(lsReq...)

		// The cleanup from the last "It" should make this return nothing.
		Eventually(func(g Gomega) {
			cachedObjects, err := dynamicWatcher.List(watcher, cm2.GroupVersionKind(), namespace, ls)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cachedObjects).To(BeEmpty())
		}, "10s").Should(Succeed())

		_, err = k8sClient.CoreV1().ConfigMaps(namespace).Create(ctx, &cm2, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func(g Gomega) {
			cachedObjects, err := dynamicWatcher.List(watcher, cm2.GroupVersionKind(), namespace, ls)
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(cachedObjects).To(HaveLen(1))
			g.Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(3)))
			g.Expect(cachedObjects[0].GroupVersionKind()).To(Equal(cm2.GroupVersionKind()))
			g.Expect(cachedObjects[0].GetNamespace()).To(Equal(cm2.GetNamespace()))
			g.Expect(cachedObjects[0].GetName()).To(Equal(cm2.GetName()))
		}, "5s").Should(Succeed())
	})

	It("Returns the list from the cache", func() {
		lsReq, err := labels.ParseToRequirements("watch=me")
		Expect(err).ToNot(HaveOccurred())
		ls := labels.NewSelector().Add(lsReq...)

		gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

		cachedObjects, err := dynamicWatcher.ListFromCache(gvk, namespace, ls)
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObjects).To(HaveLen(1))
		Expect(cachedObjects[0].GroupVersionKind()).To(Equal(gvk))
		Expect(cachedObjects[0].GetNamespace()).To(Equal(namespace))
		Expect(cachedObjects[0].GetName()).To(Equal("configmap-for-list2"))
	})

	It("Allows the list query batch to end", func() {
		DeferCleanup(func(dctx context.Context) {
			err := k8sClient.CoreV1().ConfigMaps(namespace).Delete(dctx, "configmap-for-list2", metav1.DeleteOptions{})
			if !k8serrors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		})

		watcher := toObjectIdentifer(watched[1])
		Expect(dynamicWatcher.EndQueryBatch(watcher)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(3)))

		Expect(dynamicWatcher.RemoveWatcher(watcher)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Does not allow directly updating watches when a query batch is in progress", func() {
		watched := toObjectIdentifer(watched[0])
		Expect(dynamicWatcher.AddWatcher(watcherID, watched)).To(MatchError(ErrQueryBatchInProgress))
		Expect(dynamicWatcher.AddOrUpdateWatcher(watcherID, watched)).To(MatchError(ErrQueryBatchInProgress))
		Expect(dynamicWatcher.RemoveWatcher(watcherID)).To(MatchError(ErrQueryBatchInProgress))
	})

	It("Perform another get query", func() {
		cachedObj2, err := dynamicWatcher.Get(
			watcherID, watched[1].GroupVersionKind(), watched[1].GetNamespace(), watched[1].GetName(),
		)
		Expect(err).ToNot(HaveOccurred())

		Expect(cachedObj2.GroupVersionKind()).To(Equal(watched[1].GroupVersionKind()))
		Expect(cachedObj2.GetNamespace()).To(Equal(watched[1].GetNamespace()))
		Expect(cachedObj2.GetName()).To(Equal(watched[1].GetName()))
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(3)))
	})

	It("Allows the get query batch to end", func() {
		Expect(dynamicWatcher.EndQueryBatch(watcherID)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(2)))
	})

	It("Cleans up the watches for an empty query batch", func() {
		Expect(dynamicWatcher.StartQueryBatch(watcherID)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.EndQueryBatch(watcherID)).ToNot(HaveOccurred())

		listWatcher := toObjectIdentifer(watched[1])
		Expect(dynamicWatcher.StartQueryBatch(listWatcher)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.EndQueryBatch(listWatcher)).ToNot(HaveOccurred())

		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(0)))
	})

	It("Ensures watch references are correct for two watchers on same object", func() {
		Expect(dynamicWatcher.StartQueryBatch(watcherID)).ToNot(HaveOccurred())
		_, err := dynamicWatcher.Get(
			watcherID, watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(dynamicWatcher.EndQueryBatch(watcherID)).ToNot(HaveOccurred())

		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))

		otherWatcher := toObjectIdentifer(watched[1])
		Expect(dynamicWatcher.StartQueryBatch(otherWatcher)).ToNot(HaveOccurred())
		_, err = dynamicWatcher.Get(
			otherWatcher, watched[0].GroupVersionKind(), watched[0].GetNamespace(), watched[0].GetName(),
		)
		Expect(err).ToNot(HaveOccurred())
		Expect(dynamicWatcher.EndQueryBatch(otherWatcher)).ToNot(HaveOccurred())

		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))

		// Removing the original watcher should not remove the watch reference.
		Expect(dynamicWatcher.RemoveWatcher(watcherID)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))

		// Removing the last watcher should remove the watch.
		Expect(dynamicWatcher.RemoveWatcher(otherWatcher)).ToNot(HaveOccurred())
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(0)))
	})

	It("Allows a GVK to GVR conversion using the object cache", func() {
		gvr, err := dynamicWatcher.GVKToGVR(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"})
		Expect(err).ToNot(HaveOccurred())
		Expect(gvr.Namespaced).To(BeTrue())
		Expect(gvr.Resource).To(Equal("configmaps"))
	})

	It("A cluster scoped Get query with a namespace is handled correctly", func(ctx SpecContext) {
		By("Performing a first query batch")
		nsGVK := schema.GroupVersionKind{Kind: "Namespace", Version: "v1"}

		err := dynamicWatcher.StartQueryBatch(watcherID)
		Expect(err).ToNot(HaveOccurred())

		defaultNS, err := dynamicWatcher.Get(watcherID, nsGVK, "some-namespace", "default")
		Expect(err).ToNot(HaveOccurred())

		err = dynamicWatcher.EndQueryBatch(watcherID)
		Expect(err).ToNot(HaveOccurred())

		Expect(defaultNS.GetName()).To(Equal("default"))
		Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))

		By("Performing a second duplicate query batch to ensure watches are not cleaned up")
		err = dynamicWatcher.StartQueryBatch(watcherID)
		Expect(err).ToNot(HaveOccurred())

		_, err = dynamicWatcher.Get(watcherID, nsGVK, "some-namespace", "default")
		Expect(err).ToNot(HaveOccurred())

		err = dynamicWatcher.EndQueryBatch(watcherID)
		Expect(err).ToNot(HaveOccurred())

		Consistently(func(g Gomega) {
			g.Expect(dynamicWatcher.GetWatchCount()).To(Equal(uint(1)))
		}, "5s").Should(Succeed())

		By("Performing an update to trigger a reconcile")

		realDefaultNS, err := k8sClient.CoreV1().Namespaces().Get(ctx, "default", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		annotations := realDefaultNS.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}

		annotations["my-test"] = "test"

		realDefaultNS.SetAnnotations(annotations)

		_, err = k8sClient.CoreV1().Namespaces().Update(ctx, realDefaultNS, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		// One reconcile for the initial reconcile and the other for the update
		Eventually(reconcilerObj.ResultsChan, "3s").Should(HaveLen(2))

		By("Pulling from the cache without the namespace")
		ns, err := dynamicWatcher.GetFromCache(nsGVK, "", "default")
		Expect(err).ToNot(HaveOccurred())
		Expect(ns).ToNot(BeNil())
	})
})
