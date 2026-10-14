# Jepsen, the campaign of 2.3.0, 2026-10-13 and 14

Copied from the Jepsen control node before the droplets were released. One control node ran the
harness for one set of six test nodes, `n1` to `n6`, all DigitalOcean droplets with 2 vCPU and 4 GB
(the control was resized to 16 GB on 2026-10-14, see below). The harness is in git (`test/jepsen`).
This folder holds what git does not.

## Result

**244 tests, all valid**: the same 244 that the code of 2.2.0 passed (`../2026-10-09-jepsen-220`).

- **Main list**, 213 tests (`test/jepsen/matrix-220.txt` unchanged), on `n1` to `n5`: 213 valid.
  Three of them first read UNKNOWN because the checker of the log workload ran out of memory on the
  control (`OutOfMemoryError: Java heap space` at the 2.6 GB heap a 4 GB control allows; the other
  checkers of those tests valid, "no anomalies found"). Their stored histories were checked again
  with 8 GB (`lein run analyze -t <store>`): valid, with 0 anomalies, 0 duplicates and 0 torn reads
  in the log checker. `main/results/<test>.edn` is the second analysis; the first is kept as
  `<test>.unknown-2600m.edn`, and `main/reanalyze.txt` has both.
- **Failover**, 31 tests (`test/jepsen/matrix-220b2.txt`, W12: a standby cluster's failover, the
  source on `n1` to `n3`, the standby on `n4` to `n6`): 31 valid. One of them,
  `fo-crash-lz-kill`, first crashed in the harness before any load: its setup timed out while
  `n6`, new to the set, built lazyfs for the first time (the 2.2.0 campaign met the same on its
  sixth node). lazyfs was then built on `n6` by the harness's own recipe, and the test ran again at
  the end of the list as `fo-crash-lz-kill-r2`: valid. `failover/notes.txt` says so.

## The build

`/root/bin/queen-230b2`, md5 `4a3ebe9aa913b0d2f0b03c72f5abe56a` (`binaries.md5`): the broker
binary of the published image `ghcr.io/queen-mq/queen:2.3.0-beta.2`
(`sha256:bec6402707fe3d2913a6b5cda87776f0138dac46b585ffd37ef0f40ade75465e`), built from
`1c7136d4a`. The image `2.3.0` (built from `fd5e2c360`, on stage since 2026-10-13 13:32 UTC and on
prod since 2026-10-14 05:42 UTC) has the same broker code: between the two commits only the version
line of `server/Cargo.toml` changed under `server/`, `proxy/`, `protocols/`, `crates/` and
`connectors/`.

The harness is `test/jepsen` of `abccef016` with one change, in `project.clj`: the checker's heap
was `-Xmx2600m` until the resize, then `-Xmx12g` again (`scripts/project.clj.as-run`).

## Timeline (UTC)

| When | What |
|---|---|
| 10-13 11:52 | a setup check (`s-w8-none`): UNKNOWN only because `gnuplot` was missing on the control; installed; valid at 11:58 (`main/summary-setup-check.txt`, `main/summary-smoke.txt`) |
| 10-13 11:58 | the main list starts (`scripts/campaign-230.sh`) |
| 10-14 05:15 | the main list stops after its 176th test, cleanly, for the control's resize to 16 GB |
| 10-14 05:18 | `scripts/resume-230.sh`: the three histories checked again, the 37 tests left (`main/matrix-after-resize.txt`), then the failover list |
| 10-14 08:54 | the main list ends: 213 valid |
| 10-14 11:50 | the failover list ends: 31 valid |

## What it does not cover

The binary wire forms of 2.3.0 (binary push, compressed groups, framed pop, the binary transaction
body): the harness's client speaks JSON. The Kafka facade: no Jepsen workload uses it.

## Files

| Path | Contents |
|---|---|
| `main/`, `failover/` | one folder per list |
| `*/summary.txt` | one `start` and one `done` line per test, as the runner wrote them (the three UNKNOWN and the CRASHED stay as they were) |
| `*/results/<test>.edn` | each test's `results.edn` |
| `*/matrix.txt` | the tests of each list (`failover/matrix.txt` ends with the rerun) |
| `main/reanalyze.txt` | the second analysis of the three histories |
| `failover/notes.txt` | why `fo-crash-lz-kill` ran twice |
| `scripts/` | the launchers, the resume script, the re-analysis, `collect-jepsen-230.sh` (this folder) |
| `binaries.md5` | the checksum of the build |

The Jepsen stores (every history, the Jepsen logs and each node's `queen.log`) were not copied.
