# Execution ledger — plan: docs/superpowers/plans/2026-11-25-fraud-monitoring.md

Design approved 2026-11-25. User delegated subsequent technical choices.
Pre-flight: Tasks 1→2 share enriched output fields; Tasks 1/2→3 share metric names; Task 3→4 shares datasource/dashboard UIDs. No conflicts.
Ruling: independent HW-5 repository on feat/monitoring is the isolation boundary; no linked worktree required because this is a new project.
Ruling: Prometheus localhost:9091 avoids AdGuard VPN localhost:9090. PostgreSQL localhost:5433 avoids other homework defaults.
Baseline: HW-3 source copied from git-tracked files only; no existing automated suite. Docker daemon started; no running containers.
Task 1: complete — 4 scoring tests RED then GREEN. IDs/context preserved, metrics measured, reload failure retains model. Local venv uses existing host scientific packages; runtime uses pinned container dependencies.
Task 2: complete — 2 PostgreSQL integration tests RED→GREEN, suite 6 passed. Duplicate writes excluded; rollback on failed insert. Ruling: DB outage triggers process restart without offset commit (reconnect/replay via restart policy), simpler than in-loop retry. Initial compose build found missing trailing newline in inherited requirements; corrected requirements entries.
