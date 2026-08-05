---
name: verify
description: Run the full cache-kit verification pass (build, vet, race-enabled tests) and summarize results. Use before every commit/push or when asked "does it still pass".
---

# Verify cache-kit

Run, in order, stopping at the first failure:

```bash
go build ./...
go vet ./...
go test -race -count=1 ./...
```

Notes:

- `-race` is mandatory — the package's main risks are concurrency bugs in
  the single-flight path (`group.go`, `GetOrSet`).
- `-count=1` avoids cached test results masking flakiness in the
  concurrency tests.
- Tests use miniredis; if a test tries to dial a real server, that is a bug
  in the test, not an environment problem.
- If `TestGetOrSetSingleFlight` flakes, re-run it with
  `go test -race -run TestGetOrSetSingleFlight -count=20 .` and report the
  failure rate instead of ignoring it.

Report: pass/fail per step, and on failure the relevant output trimmed to
the failing test or vet finding, plus which CLAUDE.md invariant (if any) the
failure points at.
