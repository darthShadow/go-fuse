# Benchmark Profiles

CI uploads one benchmark profile artifact per Go/GOMAXPROCS matrix leg.

Artifact names use this scheme:

`bench-profiles-go${{ matrix.go.label }}-gomaxprocs${{ matrix.GOMAXPROCS }}`

Each artifact contains the matching `benchmark/profiles/` subdirectory:

`go${{ matrix.go.label }}-gomaxprocs${{ matrix.GOMAXPROCS }}/`

For example, the `bench-profiles-go1.27-gomaxprocs0` artifact contains
`go1.27-gomaxprocs0/`. When `GO_PROFILE_TAG` is unset, `all.bash` falls back
to `PROFILE_TAG=dev`, so a local run writes `godev-gomaxprocs<N>/`, where
`<N>` is `GOMAXPROCS` or `0` when it is unset.

That directory contains `cpu.pprof`, `mem.pprof`, `mutex.pprof`,
`block.pprof`, and `benchmark.test`.

After downloading an artifact, inspect a profile from the inner directory:

`go tool pprof -http=: benchmark.test cpu.pprof`
