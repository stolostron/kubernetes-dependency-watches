// Copyright Contributors to the Open Cluster Management project
package client

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

var _ = Describe("Test the cache", Ordered, func() {
	var cache ObjectCache
	configMapGVK := schema.GroupVersionKind{
		Version: "v1",
		Kind:    "ConfigMap",
	}

	BeforeEach(func() {
		discoveryClient, err := discovery.NewDiscoveryClientForConfig(k8sConfig)
		Expect(err).ToNot(HaveOccurred())

		cache = NewObjectCache(discoveryClient, ObjectCacheOptions{})
	})

	It("Returns an error for queries with no cache entries", func() {
		object, err := cache.Get(configMapGVK, "default", "something")
		Expect(err).To(MatchError(ErrNoCacheEntry))
		Expect(object).To(BeNil())

		objects, err := cache.List(configMapGVK, "default", nil)
		Expect(err).To(MatchError(ErrNoCacheEntry))
		Expect(objects).To(BeNil())
	})

	It("Caches a get object", func() {
		object := unstructured.Unstructured{}
		object.SetName("cached-object1")
		cache.CacheObject(configMapGVK, "default", "something", &object)

		cachedObject, err := cache.Get(configMapGVK, "default", "something")
		Expect(err).ToNot(HaveOccurred())
		Expect(*cachedObject).To(Equal(object))

		// Update the cache and get again
		object.SetLabels(map[string]string{"raleigh": "nc"})
		cache.CacheObject(configMapGVK, "default", "something", &object)

		cachedObject, err = cache.Get(configMapGVK, "default", "something")
		Expect(err).ToNot(HaveOccurred())
		Expect(*cachedObject).To(Equal(object))

		cache.UncacheObject(configMapGVK, "default", "something")
		cachedObject, err = cache.Get(configMapGVK, "default", "something")
		Expect(err).To(MatchError(ErrNoCacheEntry))
		Expect(cachedObject).To(BeNil())
	})

	It("Caches a list query result", func() {
		object1 := unstructured.Unstructured{}
		object1.SetName("cached-object1")
		object2 := unstructured.Unstructured{}
		object2.SetName("cached-object2")
		cache.CacheList(configMapGVK, "default", nil, []unstructured.Unstructured{object1, object2})

		cachedObjects, err := cache.List(configMapGVK, "default", nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObjects).To(HaveLen(2))
		Expect(cachedObjects[0]).To(Equal(object1))
		Expect(cachedObjects[1]).To(Equal(object2))

		// Simulate an object being deleted
		cache.CacheList(configMapGVK, "default", nil, []unstructured.Unstructured{object2})

		cachedObjects, err = cache.List(configMapGVK, "default", nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObjects).To(HaveLen(1))
		Expect(cachedObjects[0]).To(Equal(object2))

		cache.UncacheList(configMapGVK, "default", nil)
		cachedObjects, err = cache.List(configMapGVK, "default", nil)
		Expect(err).To(MatchError(ErrNoCacheEntry))
		Expect(cachedObjects).To(BeNil())
	})

	It("Can convert a GVK to a GVR", func() {
		gvr, err := cache.GVKToGVR(configMapGVK)
		Expect(err).ToNot(HaveOccurred())
		Expect(gvr.Resource).To(Equal("configmaps"))
	})

	It("Fails when a resource is invalid when converting a GVK to a GVR", func() {
		// Invalid group
		_, err := cache.GVKToGVR(schema.GroupVersionKind{Group: "Italian Food", Version: "v1", Kind: "Spaghetti"})
		Expect(err).To(MatchError(ErrNoVersionedResource))

		// Valid group but invalid kind
		_, err = cache.GVKToGVR(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Spaghetti"})
		Expect(err).To(MatchError(ErrNoVersionedResource))
	})

	It("Does not convert to GVRs that can't be watched", func() {
		_, err := cache.GVKToGVR(schema.GroupVersionKind{
			Group:   "authorization.k8s.io",
			Version: "v1",
			Kind:    "SubjectAccessReview",
		})
		Expect(err).To(MatchError(ErrNoVersionedResource))
	})

	It("Can have its cache cleared", func() {
		gvr, err := cache.GVKToGVR(configMapGVK)
		Expect(err).ToNot(HaveOccurred())
		Expect(gvr.Resource).To(Equal("configmaps"))

		object1 := unstructured.Unstructured{}
		object1.SetName("cached-object1")
		object2 := unstructured.Unstructured{}
		object2.SetName("cached-object2")
		cache.CacheObject(configMapGVK, "default", "something1", &object1)
		cache.CacheObject(configMapGVK, "default", "something2", &object2)

		cache.Clear()

		cachedObject1, err := cache.Get(configMapGVK, "default", "something1")
		Expect(err).To(MatchError(ErrNoCacheEntry))
		Expect(cachedObject1).To(BeNil())

		cachedObject2, err := cache.Get(configMapGVK, "default", "something2")
		Expect(err).To(MatchError(ErrNoCacheEntry))
		Expect(cachedObject2).To(BeNil())
	})

	It("Can have an empty cache entry", func() {
		cache.CacheList(configMapGVK, "default", nil, nil)

		cachedObjects, err := cache.List(configMapGVK, "default", nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObjects).To(BeEmpty())

		obj := ObjectIdentifier{
			Group:     "",
			Version:   "v1",
			Kind:      "ConfigMap",
			Namespace: "test-ns",
			Name:      "watcher",
		}
		cache.CacheFromObjectIdentifier(obj, nil)

		cachedObject, err := cache.Get(configMapGVK, "test-ns", "watcher")
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObject).To(BeNil())
	})
})

var _ = Describe("Test the UnsafeDisableDeepCopy option of the cache", func() {
	var discoveryClient *discovery.DiscoveryClient
	configMapGVK := schema.GroupVersionKind{
		Version: "v1",
		Kind:    "ConfigMap",
	}

	BeforeEach(func() {
		var err error
		discoveryClient, err = discovery.NewDiscoveryClientForConfig(k8sConfig)
		Expect(err).ToNot(HaveOccurred())
	})

	It("Defaults to DeepCopy, preventing mutations", func() {
		cache := NewObjectCache(discoveryClient, ObjectCacheOptions{})

		object := unstructured.Unstructured{}
		object.SetName("cached-object1")
		object.SetLabels(map[string]string{"mylabel": "one"})
		cache.CacheObject(configMapGVK, "default", "cached-object1", &object)

		cachedObject1, err := cache.Get(configMapGVK, "default", "cached-object1")
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObject1.GetLabels()).To(HaveKeyWithValue("mylabel", "one"))

		cachedObject1.SetLabels(map[string]string{"mylabel": "two"})

		cachedObject2, err := cache.Get(configMapGVK, "default", "cached-object1")
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObject2.GetLabels()).To(HaveKeyWithValue("mylabel", "one"))
	})

	It("Can be set to disable the DeepCopy, allowing mutations", func() {
		cache := NewObjectCache(discoveryClient, ObjectCacheOptions{UnsafeDisableDeepCopy: true})

		object := &unstructured.Unstructured{}
		object.SetName("cached-object1")
		object.SetLabels(map[string]string{"mylabel": "one"})
		cache.CacheObject(configMapGVK, "default", "cached-object1", object)

		cachedObject1, err := cache.Get(configMapGVK, "default", "cached-object1")
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObject1.GetLabels()).To(HaveKeyWithValue("mylabel", "one"))

		cachedObject1.SetLabels(map[string]string{"mylabel": "two"})

		cachedObject2, err := cache.Get(configMapGVK, "default", "cached-object1")
		Expect(err).ToNot(HaveOccurred())
		Expect(cachedObject2.GetLabels()).To(HaveKeyWithValue("mylabel", "two"))
	})
})
