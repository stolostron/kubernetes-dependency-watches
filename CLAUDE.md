# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Overview

This is an event-driven Go library for tracking dependencies between Kubernetes objects. When a
Kubernetes object needs to monitor changes to other objects (e.g., a ConfigMap watching a Secret),
this library provides an efficient watch mechanism with optional caching.

The library is designed as an alternative/complement to controller-runtime and can be integrated
with controller-runtime as a Channel Source.

## Core Architecture

### Main Components

**DynamicWatcher** (`client/client.go`): The central interface and implementation

- Manages dynamic watches on Kubernetes objects using the Kubernetes watch API
- Uses a queue-based reconciliation pattern similar to controller-runtime
- Maintains bidirectional mappings:
  - `watchedToWatchers`: maps watched objects to watchers that care about them
  - `watcherToWatches`: maps watcher objects to the objects they watch
  - `watches`: maps watched objects to their active API watch requests

**ObjectCache** (`client/cache.go`): Thread-safe caching layer

- Caches watched Kubernetes objects using sync.Map for concurrency
- Implements GVK to GVR conversion with configurable TTL (default 10 minutes)
- Automatically strips managedFields and last-applied-configuration to save memory
- Supports both individual object caching and list query caching

**ControllerRuntimeSource** (`client/source.go`): Integration adapter

- Bridges DynamicWatcher with controller-runtime controllers
- Converts ObjectIdentifier events to controller-runtime GenericEvents
- Uses a buffered channel (default 1024 items) to relay reconcile requests

### Key Concepts

**ObjectIdentifier**: Identifies a Kubernetes object or set of objects

- Single object: specified by Group/Version/Kind/Namespace/Name
- Multiple objects: specified by Group/Version/Kind/Namespace/Selector

**Watch Lifecycle**:

1. `AddWatcher` or `AddOrUpdateWatcher` creates watches
2. `relayWatchEvents` runs in goroutines, monitoring watch channels
3. On watch events, watchers get added to the reconcile queue
4. Watch automatically restarts if it fails (e.g., resource version expired)
5. `RemoveWatcher` cleans up watches when no longer referenced

**Query Batch Pattern**: For caching use cases

1. `StartQueryBatch` begins a transaction
2. `Get`/`List` calls add watches and return cached objects
3. `EndQueryBatch` removes watches no longer referenced in the batch

## Common Development Commands

### Testing

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage

# Test arguments can be customized
TESTARGS="-v -ginkgo.focus='My Test'" make test
```

### Linting and Formatting

```bash
# Run all linters
make lint

# Format code
make fmt

# Security scan
make gosec-scan
```

### Dependencies

```bash
# Install test dependencies (Ginkgo, envtest)
make e2e-dependencies
make envtest
```

## Testing Guidelines

- Tests use Ginkgo/Gomega with envtest for a real Kubernetes API server
- The `ENVTEST_K8S_VERSION` is auto-determined from go.mod (k8s.io/api version)
- Tests are in `*_test.go` files, with `suite_test.go` setting up the test environment
- See `client_example_test.go` for comprehensive usage examples

## Important Implementation Details

**Watch Restart Behavior** (`client.go:309-422`):

- Watches automatically restart if they fail unexpectedly
- If a watch stops due to forbidden/unauthorized errors, the watch is cleaned up and watchers are
  reconciled
- On restart, an initial event is sent to account for potentially lost events
- Uses client-go's RetryWatcher for built-in resilience

**Concurrency Safety**:

- All shared state protected by RWMutex or sync.Map
- Watch goroutines are the only writers to their watch channels
- Cache operations use sync.Map for lock-free reads
- Query batches use RWMutex to coordinate batch completion

**Error Handling**:

- Custom errors defined at top of `client.go`: `ErrNotStarted`, `ErrCacheDisabled`,
  `ErrQueryBatchInProgress`, etc.
- Always check for specific errors using `errors.Is()`
- Watch errors are logged but don't stop the DynamicWatcher

## Integration Patterns

**Standalone Usage**:

```go
dynamicWatcher, _ := client.New(k8sConfig, reconciler, nil)
go dynamicWatcher.Start(ctx)
<-dynamicWatcher.Started()
dynamicWatcher.AddWatcher(watcher, watched)
```

**Controller-Runtime Integration**:

```go
reconciler, sourceChan := client.NewControllerRuntimeSource()
dynamicWatcher, _ := client.New(k8sConfig, reconciler, nil)
go dynamicWatcher.Start(ctx)

ctrl.NewControllerManagedBy(mgr).
    For(&MyType{}).
    WatchesRawSource(sourceChan).
    Complete(myReconciler)
```
