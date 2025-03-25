// Copyright Contributors to the Open Cluster Management project

package client

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const namespace = "test-ns"

var (
	k8sConfig   *rest.Config
	k8sClient   *kubernetes.Clientset
	noWatchUser *envtest.AuthenticatedUser
	testEnv     *envtest.Environment
	ctx, cancel = context.WithCancel(context.TODO())
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	format.TruncatedDiff = false

	By("bootstrapping the test environment")
	testEnv = &envtest.Environment{ControlPlaneStopTimeout: time.Minute * 3}

	var err error

	k8sConfig, err = testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(k8sConfig).NotTo(BeNil())

	noWatchUser, err = testEnv.AddUser(envtest.User{Name: "no-watch"}, nil)
	Expect(err).ToNot(HaveOccurred())

	// Required for tests that involve restarting the test environment since new certs are generated.
	k8sConfig.TLSClientConfig.Insecure = true
	k8sConfig.TLSClientConfig.CAData = nil

	k8sClient, err = kubernetes.NewForConfig(k8sConfig)
	Expect(err).ToNot(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	_, err = k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())

	listAllRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "list-all"},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:     []string{"list"},
				Resources: []string{"*"},
				APIGroups: []string{"*"},
			},
		},
	}

	_, err = k8sClient.RbacV1().ClusterRoles().Create(ctx, listAllRole, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())

	listAllBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "list-all"},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     listAllRole.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "User",
				Name:     "no-watch",
			},
		},
	}

	_, err = k8sClient.RbacV1().ClusterRoleBindings().Create(ctx, listAllBinding, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := k8sClient.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	Expect(err).ToNot(HaveOccurred())

	err = k8sClient.RbacV1().ClusterRoleBindings().Delete(ctx, "list-all", metav1.DeleteOptions{})
	Expect(err).ToNot(HaveOccurred())

	err = k8sClient.RbacV1().ClusterRoles().Delete(ctx, "list-all", metav1.DeleteOptions{})
	Expect(err).ToNot(HaveOccurred())

	cancel()

	err = testEnv.Stop()
	Expect(err).ToNot(HaveOccurred())
})
