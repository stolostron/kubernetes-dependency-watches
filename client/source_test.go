// Copyright Contributors to the Open Cluster Management project

package client

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type controllerRuntimeReconciler struct {
	ReconcileCount int
}

func (r *controllerRuntimeReconciler) Reconcile(_ context.Context, _ reconcile.Request) (reconcile.Result, error) {
	r.ReconcileCount++

	return reconcile.Result{}, nil
}

var _ = Describe("Test the controller-runtime source wrapper", func() {
	var (
		ctxTest               context.Context
		cancelCtxTest         context.CancelFunc
		dynamicWatcher        DynamicWatcher
		ctrlRuntimeReconciler controllerRuntimeReconciler
		watched               []k8sObject
		watchedObjIDs         []ObjectIdentifier
		watcher               *corev1.ConfigMap
	)

	BeforeEach(func() {
		ctxTest, cancelCtxTest = context.WithCancel(ctx)

		ctrlRuntimeReconciler = controllerRuntimeReconciler{}
		reconciler, sourceChan := NewControllerRuntimeSource()
		watcher, watched, dynamicWatcher = getDynamicWatcher(ctxTest, reconciler)

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

		options := ctrl.Options{
			Namespace:              namespace,
			Scheme:                 scheme.Scheme,
			MetricsBindAddress:     "0",
			HealthProbeBindAddress: "0",
			LeaderElection:         false,
		}
		mgr, err := ctrl.NewManager(k8sConfig, options)
		Expect(err).ToNot(HaveOccurred())

		err = ctrl.NewControllerManagedBy(mgr).
			For(&corev1.ConfigMap{}).
			Watches(sourceChan, &handler.EnqueueRequestForObject{}).
			Complete(&ctrlRuntimeReconciler)
		Expect(err).ToNot(HaveOccurred())

		go func() {
			defer GinkgoRecover()

			err := mgr.Start(ctxTest)
			Expect(err).ToNot(HaveOccurred())
		}()

		// Wait for the list reconcile
		Eventually(func() int { return ctrlRuntimeReconciler.ReconcileCount }, "5s").Should(Equal(1))

		// Reset the reconcile count so we are only tracking reconciles triggered by watches
		ctrlRuntimeReconciler.ReconcileCount = 0
	})

	AfterEach(func() {
		err := k8sClient.CoreV1().Secrets(namespace).Delete(ctxTest, watched[0].GetName(), metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}

		err = k8sClient.RbacV1().ClusterRoles().Delete(ctxTest, watched[1].GetName(), metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}

		err = k8sClient.CoreV1().ConfigMaps(namespace).Delete(ctxTest, watcher.Name, metav1.DeleteOptions{})
		if !k8serrors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred())
		}

		cancelCtxTest()
	})

	It("Verifies that a controller-runtime reconciler can use this library", func() {
		By("Adding the watcher with a single watched object")
		err := dynamicWatcher.AddOrUpdateWatcher(toObjectIdentifer(watcher), watchedObjIDs[0])
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() int { return ctrlRuntimeReconciler.ReconcileCount }, "5s").Should(Equal(1))

		By("Updating a watched object to trigger a reconcile")
		watchedSecret := watched[0].(*corev1.Secret)
		watchedSecret.StringData = map[string]string{"hello": "world"}
		watched[0], err = k8sClient.CoreV1().Secrets(namespace).Update(ctxTest, watchedSecret, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("Verifying that the controller-runtime reconciler was triggered")
		Eventually(func() int { return ctrlRuntimeReconciler.ReconcileCount }, "5s").Should(Equal(2))
	})
})
