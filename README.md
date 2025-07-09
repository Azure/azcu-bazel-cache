# Bazel Azure Blob Storage Cache

This an an implementation of the bazel remote cache that stores data in Azure
Blob storage.

I would generally consider this feature complete (outside of exposing certain
configuration options). This is not currently being used in production, but I
have done a fair amount of testing with it (both automated and manual).

There are two components:

### Proxy

The proxy is meant to be run local to where bazel is executing.
Configure bazel to send cache to the proxy, and the proxy can send to the actual
caching backend.

The proxy is reseponsible for authenticating with Azure and injecting those
short-lived authentication tokens along in the GRPC request metadata.

Note: bazel itself is (currently) only capable of doing basic auth. This is
where the proxy comes in to inject azure auth tokens into the GRPC request sent
by bazel.

### Server

The server is responsible for managing the data in the azure blob store.
This is what actually does the work of the bazel remote caches.

The server will look for authentication tokens from the GRPC request metadata.

Bazel has two sets of caches:

1. Action cache - Bazel hashes all the build inputs into a digest. The digest
used to lookup action cache data which contains digests of all the outputs. In
effect the action cache acts as a cache index for a set of build inputs so that
bazel can find *what* is cached. These should all typically be small files.

2. CAS - The CAS (or content addressable store) is (generally) what the action
cache points to. It is where all the data that bazel wants to cache lives. Files
can range from very small to very large.

The server allows you to choose how to store each of these, e.g. what storage
account, container, and path.
