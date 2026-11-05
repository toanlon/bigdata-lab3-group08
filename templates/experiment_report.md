# Lab 3 experiment report
Group / run label / author / reviewer:

## 1. Upstream handoff
State whether you used your approved Lab 2 Parquet or the course fallback.
Record the independently supplied manifest hash, source release ID, schema,
AS_OF, and logical baseline hash. Do not claim the course fixture was signed
or published by your group. Explain where the original Lab 2 raw data remain.

## 2. Table semantics and lineage
Record Bronze, Silver and Gold URIs and table versions. Explain a Bronze
receipt, a Silver business-key winner and a Gold site/day aggregation.
Trace one updated row to source object, physical row and SHA-256.
Draw only actual read/write edges; governance metadata are not data carriers.

## 3. Incremental evidence
Report incoming physical records, per-key winners, inserts, updates,
unchanged/stale records, Bronze count and Silver count. Show the replay
and time-travel results. Distinguish logical equivalence from Delta version
identity and Parquet file-byte identity. Explain the invalid-batch policy.

## 4. Analytics
Use the three saved SQL outputs. Explain why observation-weighted mean
is generally different from AVG(site means). Report numerators/denominators
for late fraction and >25 C fraction. Interpret >25 C as a teaching threshold,
not a calibrated physical safety threshold. State residual QA limitations.

## 5. Controlled layout experiment
Record exact dataset version, copy factor, expanded rows, resource settings,
setup times, total table files/bytes, execution plans, all six elapsed times,
medians, and A-median/B-median. Identify PartitionFilters in B's plan.
Do NOT call total table files 'files scanned'. Do NOT call repeated runs cold.
Explain the outcome even if layout B is slower. Synthetic copies are not new
independent observations and must never enter operational Gold.

## 6. Release and accountability
Record the pinned Silver/Gold pair and generating-code hash; validator code
may have its own hash. Attach five release-gate outcomes and the owner's
independent result. List who authored/reviewed each technical contribution.
Do not include credentials, kubeconfig, or restricted raw payloads.
