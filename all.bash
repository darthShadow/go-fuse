#!/bin/bash
set -eux

# Kernel version is relevant to debugging CI failures
uname -a

# Everything must compile on Linux
go build ./...

# Not everything compiles on MacOS (try GOOS=darwin go build ./...).
# But our key packages should.
GOOS=darwin go build ./fs/... ./example/loopback/...
GOOS=freebsd go build ./fs/... ./example/loopback/...

# Run the tests. Why the flags:
# -timeout 5m ... Get a backtrace on a hung test before the CI system kills us
# -p 1 .......... Run tests serially, which also means we get live output
#                 instead of per-package buffering.
# -count 1 ...... Disable result caching, so we can see flakey tests
GO_TEST="go test -timeout 5m -p 1 -count 1"
PROFILE_TAG="${GO_PROFILE_TAG:-dev}"
PROFILE_GOMAXPROCS="${GOMAXPROCS:-0}"
# Run all tests as current user
$GO_TEST ./...
# The following tests need to run as root
sudo env PATH=$PATH $GO_TEST -run 'Test(DirectMount|Forget|Passthrough|IDMappedMount)' ./fs ./fuse

# Race detector on the concurrency-critical bridge package (Artifact A lock-free rework).
# Skip the GOMAXPROCS=1 matrix leg: with a single P the detector observes almost no
# goroutine interleaving, which is exactly what this gate must exercise.
if [ "${PROFILE_GOMAXPROCS}" != "1" ]; then
	$GO_TEST -race ./fs ./fuse
fi

# Run virtiofs tests (including posixtest inside a VM) if QEMU and KVM are available.
# These are skipped automatically by TestMain when assets cannot be prepared.
$GO_TEST -timeout 2m -run 'Test(Basic|Posixtest)' ./virtiofs

make -C benchmark

# Pass 1: authoritative timing without profiling overhead.
go test ./benchmark -test.run '^$' -test.bench '.*' -test.benchmem -test.cpu 1,2

# Pass 2: one profile set for the job's ambient GOMAXPROCS.
PROF_DIR="benchmark/profiles/go${PROFILE_TAG}-gomaxprocs${PROFILE_GOMAXPROCS}"
mkdir -p "${PROF_DIR}"
go test ./benchmark -test.run '^$' -test.bench '.*' -test.benchmem \
	-test.cpuprofile "${PROF_DIR}/cpu.pprof" \
	-test.memprofile "${PROF_DIR}/mem.pprof" \
	-test.mutexprofile "${PROF_DIR}/mutex.pprof" \
	-test.mutexprofilefraction 1 \
	-test.blockprofile "${PROF_DIR}/block.pprof" \
	-test.blockprofilerate 1 \
	-o "${PROF_DIR}/benchmark.test"
test -s "${PROF_DIR}/cpu.pprof"
test -s "${PROF_DIR}/mem.pprof"
test -s "${PROF_DIR}/mutex.pprof"
test -s "${PROF_DIR}/block.pprof"
