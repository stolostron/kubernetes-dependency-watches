// Copyright Contributors to the Open Cluster Management project

package client

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const namespace = "test-ns"

var (
	k8sConfig *rest.Config
	k8sClient *kubernetes.Clientset
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping the test environment")
	testEnv = &envtest.Environment{}

	var err error
	k8sConfig, err = testEnv.Start()
	Expect(err).To(BeNil())
	Expect(k8sConfig).NotTo(BeNil())

	k8sClient, err = kubernetes.NewForConfig(k8sConfig)
	Expect(err).To(BeNil())
	Expect(k8sClient).NotTo(BeNil())

	ctx, cancel = context.WithCancel(context.TODO())

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	_, err = k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	Expect(err).To(BeNil())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := k8sClient.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	Expect(err).To(BeNil())

	cancel()

	err = testEnv.Stop()
	Expect(err).To(BeNil())
})
