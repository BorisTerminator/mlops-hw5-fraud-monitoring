# Execution ledger — plan: docs/superpowers/plans/2026-11-25-fraud-monitoring.md

Design approved 2026-11-25. User delegated subsequent technical choices.
Pre-flight: Tasks 1→2 share enriched output fields; Tasks 1/2→3 share metric names; Task 3→4 shares datasource/dashboard UIDs. No conflicts.
Ruling: independent HW-5 repository on feat/monitoring is the isolation boundary; no linked worktree required because this is a new project.
Ruling: Prometheus localhost:9091 avoids AdGuard VPN localhost:9090. PostgreSQL localhost:5433 avoids other homework defaults.
Baseline: HW-3 source copied from git-tracked files only; no existing automated suite. Docker daemon started; no running containers.
Task 1: complete — 4 scoring tests RED then GREEN. IDs/context preserved, metrics measured, reload failure retains model. Local venv uses existing host scientific packages; runtime uses pinned container dependencies.
Task 2: complete — 2 PostgreSQL integration tests RED→GREEN, suite 6 passed. Duplicate writes excluded; rollback on failed insert. Ruling: DB outage triggers process restart without offset commit (reconnect/replay via restart policy), simpler than in-loop retry. Initial compose build found missing trailing newline in inherited requirements; corrected requirements entries.
Task 3: configuration checked by promtool and Grafana API. First full acceptance found empty Linux root FS on Docker Desktop; use actual available physical filesystems. Alert evaluation interval must be a multiple of Grafana scheduler 10s, corrected 5s→10s. Ruling: Node Exporter network namespace is bridged; add tiny textfile helper reading /proc/1/net/dev for accurate host network metrics while retaining shared compose network. rslave removed because Docker Desktop root mount is private.
Task 3: complete — Prometheus 3/3 targets UP; Grafana 4 dashboards and 2 healthy rules; every panel query executed with data. UI checked all four dashboards, state+merchant filtering reduced selected rows from thousands to 1. Task 4: 11 tests passing on Windows and pinned Linux container; real DAG publications, unchanged scorer start confirmed, 30 annotation samples. Stall and high-fraud alerts reached firing then inactive, normal threshold 0.5 restored.

Final review: fresh gpt-6.2-astra reviewer inspected dd60a32..fc8f062. No critical findings; seven material findings reproduced and fixed in one pass.
Final: fixed malformed times/IDs/nonfinite amounts and writer poison records — regression tests RED→GREEN; invalid complete batch rejected before inserts. Synchronous Kafka errors counted.
Final: fixed missing pointer and parseable unusable preprocessing — missing latest and incomplete artifacts tests RED→GREEN; smoke inference before activation, old pair retained.
Final: fixed high-fraud no-data semantics — no-data now OK, datasource errors explicit; live outage showed stall firing and high-fraud inactive.
Final: fixed manual DAG proof — exact publish_model task state and XCom inspected; skipped/rejected run tests RED→GREEN and real manual XCom verified published June version.
Final: fixed acceptance stale data — requires scored/written/DB-row growth and exact healthy alert UIDs; tests RED→GREEN and live blocked-INSERT experiment correctly failed acceptance.
Final: corrected model load table seconds→milliseconds and startup/reload labels; narrowed reader to scores; added monetary fraction per approved design with zero-turnover regression.
Ruling: model/date labels and reader scope treated as correctness/access issues; monetary ratio completes approved design. All reviewer findings addressed; deferred minors: none.
Ruling: startup race traced to stale ZooKeeper broker session when containers were force-recreated; Kafka now has graceful 30s shutdown and 6s ZK session timeout, ZK health dependency and startup health grace. Verified subsequent compose start succeeds.
Final: suite 35/35 Windows, 35/35 pinned Python 3.11 Linux. All panel queries return data, all targets up, two rules healthy/inactive, successful writes increase.
Final: declined-to-judge production HA, external alert delivery, model quality remain outside assignment. No new requirement omitted.
Task 5: complete locally — Russian README, 12-step walkthrough, verification evidence and source archive ready. Public GitHub publication is the final separate approval step in the approved design.
