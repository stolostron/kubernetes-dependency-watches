[![License](https://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# Overview

An event-driven Go library used when Kubernetes objects need to track when other objects change. The API is heavily
based on the popular [sigs.k8s.io/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) library.

It can also be easily integrated in
[sigs.k8s.io/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) as a
[Channel](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/source#Channel). This is simplified using this
library's `NewControllerRuntimeSource` function.

# API Documentation

See the [pkg.go.dev documentation](https://pkg.go.dev/github.com/stolostron/kubernetes-dependency-watches/client).

# Example

See the [example file](client/client_example_test.go) for an example of how to use the library.
