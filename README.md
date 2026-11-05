# Lab 3 — Lakehouse Processing & Analytics
**Five students | 150 minutes | English | continuation of Lab 2**

Build versioned Bronze/Silver/Gold tables from the approved Lab 2 output;
apply validated revisions; demonstrate replay, time travel and reproducible
SQL analytics; measure a layout change; prepare an independently reviewed release.

## What is provided (and what is not)
- `data/lab2_sources/`: the FOUR ORIGINAL Lab 2 input files, unchanged byte for byte.
  Raw observation files contain synthetic restricted fields. Keep these in the
  course workspace/raw area, never the analyst-readable release.
- `data/handoff/curated.csv`: a **course fallback**, reconstructed from Lab 2's
  rules. It has 9,610 rows and the same ten-column logical release schema.
  It is CSV transport, **not a fabricated Parquet file or your team's signed release**.
- `data/handoff/handoff_manifest.json`: documents the fallback and its limits.
  Prefer your team's approved `curated.parquet`; the runner supports that path.
- `data/changes/`: a NEW Lab 3-only, 140-row revision batch and its traceable source.
  This batch was not part of Lab 2. It satisfies the inherited row rules but may
  contain multiple physical revisions of a business key.
- `src/`: supplied runtime, bounded-fixture validation, Delta task runner and evidence plumbing.
- `templates/student_impl.py`: FIVE assessed TODO functions. This is not a complete solution.
- `notebooks/Lab3_Workbook.ipynb`: optional notebook guide; all outputs intentionally empty.
- `tools/`: input check, SQL/dashboard export and read-only consumer check.
- `schemas/`: fixed course contracts, table semantics and a lineage structural profile.
- No future-state answer key or Spark solution is included. The upstream fallback
  is an input for the already-completed Lab 2, not an answer to the Lab 3 tasks.

## Before class — instructor responsibility
Prepare Python 3.11, Java 17/21, PySpark 4.0.1 and Delta-Spark 4.0.1; freeze the
image and dependency versions. Cache all Delta/Hadoop AWS JARs in advance.
The Hadoop AWS connector MUST match the actual Hadoop version in Spark.
No class time is budgeted for Maven/PyPI downloads or cluster installation.

The preferred assessed deployment is a prepared Spark cluster with two executors
and the Lab 1/2 S3-compatible object store. The local configuration is a functional
fallback, not proof of multi-node execution. An S3-compatible API by itself does
not prove Delta log-store compatibility: the instructor must run a smoke test on
that exact backend. Only one Spark driver writes a given table at any moment.

Receive `trusted_lab3_inputs.json` independently from the TA. The in-package
`manifests/lab3_inputs_convenience.json` is NOT an independent trust anchor.
Do not change the trusted hashes to make a failed input check pass.

## Start from this directory
```bash
export TRUSTED=/instructor/trusted_lab3_inputs.json
python tools/check_inputs.py --trusted-manifest "$TRUSTED"
# Copy/edit config/cluster.example.json to config/team.json if using the prepared cluster.
export CFG=config/local.json
python src/run_lab.py --config "$CFG" --trusted-manifest "$TRUSTED" --step preflight
```
Set a unique `group_id`, `run_label`, shared table root and local evidence path.
Run the driver **inside** the prepared cluster network when using spark://.
Never use a driver-local file path as a distributed table root. The shared table
root must be s3a:// in the cluster configuration.

## Task commands
1. T1: check trusted inputs, runtime, worker placement and native Delta schema rejection.
2. T2: implement `build_gold`, then:
```bash
python src/run_lab.py --config "$CFG" --trusted-manifest "$TRUSTED" --step bootstrap
```
3. T3: implement `choose_revisions` and `merge_silver`, then:
```bash
python src/run_lab.py --config "$CFG" --trusted-manifest "$TRUSTED" --step incremental
```
The runner performs a real second merge/receipt replay and a versioned historical
read. It does not require a no-op merge to leave the Delta version unchanged.
4. T4: implement `benchmark_query`, then:
```bash
python tools/analytics_and_dashboard.py --config "$CFG"
python src/run_lab.py --config "$CFG" --trusted-manifest "$TRUSTED" --step benchmark
```
Use all six measured runs and both saved plans. Do not claim scanned-file counts
from total file metadata, cold-cache performance, or independent observations
from the replica-expanded benchmark. Never place benchmark rows in operational Gold.
5. T5: implement `validate_release_binding`, then:
```bash
python src/run_lab.py --config "$CFG" --trusted-manifest "$TRUSTED" --step release \
  --author student-A --reviewer student-E --steward student-B
```
Replace placeholders with real operators. This creates a **candidate**, not an
owner-published release. The TA/owner performs independent live verification,
then publishes with separate credentials. The consumer checks the published bytes.

## Use your real Lab 2 release
The TA supplies the approved SHA-256 of the single `curated.parquet` object in a
read-only text file. For a fresh run prefix, replace the bootstrap command with:
```bash
python src/run_lab.py --config "$CFG" --trusted-manifest "$TRUSTED" --step bootstrap \
  --team-parquet s3://research-release/lab2/releases/YOUR_RELEASE/curated.parquet \
  --team-data-sha256-file /instructor/approved_lab2_data_sha256.txt
```
The runner verifies actual bytes, schema and logical equivalence against the
fixed course handoff. It preserves source provenance and records the handoff mode.
A different valid Lab 2 dataset needs TA recalibration of the Lab 3 oracle; it must
NOT be silently replaced or accepted with relaxed checks. A Parquet directory
needs an explicit multi-file manifest adapter; the supplied path handles one object.

## Resume and safe failure
Complete the function for a stage before running it. A failed multi-table step
can leave unapproved staging tables: there is no cross-table transaction.
Do not manually edit `_delta_log` or run VACUUM. Ask the TA to diagnose a partial
run, or start a NEW `run_label` and evidence directory. Serial repetition of the
incremental step is safe at the business-state level; publication still requires
an independently verified, pinned version tuple.

## Evidence and assessment
Complete `templates/experiment_report.md`, `contributions.csv` and
`manual_checks.csv`. Preserve `evidence/`, including code_versions, real runtime
records, Delta versions, six timings, three SQL outputs, two plots and lineage.
70 points assess the group system; 30 assess each student's authorship,
independent two-minute demonstration and individual exit response.

## Validation status of this delivered package
The data, hashes, row-producing independent oracle, exact aggregates and 34
standard-library reference tests were executed. Python syntax was checked.
Spark/Delta/S3/Kubernetes integration was **not executed in the authoring runtime**;
there are no invented benchmark timings. The TA must pilot the exact runtime and
backend before class. See the Word handout and the private TA package.
# bigdata-lab3-group08
