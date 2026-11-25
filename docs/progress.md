# Execution ledger — plan: docs/superpowers/plans/2026-11-25-fraud-monitoring.md

Design approved 2026-11-25. User delegated subsequent technical choices.
Pre-flight: Tasks 1→2 share enriched output fields; Tasks 1/2→3 share metric names; Task 3→4 shares datasource/dashboard UIDs. No conflicts.
Ruling: independent HW-5 repository on feat/monitoring is the isolation boundary; no linked worktree required because this is a new project.
Ruling: Prometheus localhost:9091 avoids AdGuard VPN localhost:9090. PostgreSQL localhost:5433 avoids other homework defaults.
Baseline: HW-3 source copied from git-tracked files only; no existing automated suite. Docker daemon started; no running containers.
