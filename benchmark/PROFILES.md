# Benchmark Profiles

CI uploads one benchmark profile artifact per Go/GOMAXPROCS matrix leg.

Artifact names use this scheme:

`bench-profiles-go${{ matrix.go }}-gomaxprocs${{ matrix.GOMAXPROCS }}`

Each artifact contains the matching `benchmark/profiles/` subdirectory:

`go${{ matrix.go }}-gomaxprocs${{ matrix.GOMAXPROCS }}/`

That directory contains `cpu.pprof`, `mem.pprof`, `mutex.pprof`,
`block.pprof`, and `benchmark.test`.

After downloading an artifact, inspect a profile from the inner directory:

`go tool pprof -http=: benchmark.test cpu.pprof`
