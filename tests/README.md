The data-integrity check is `python tools/check_inputs.py --trusted-manifest <TA-copy>`.
It does not require Spark and does not solve the assessed functions.

The task runner produces these public test records:
- preflight: native Delta schema rejection and worker-placement probe;
- incremental: 3 isolated invalid-batch cases, physical receipt replay,
  Silver business-state replay, reordered winners and actual historical read;
- release: 1 valid plus 4 rejected snapshot/review/code-binding cases.

TA-only expected rows and KPI answers are NOT included here. Use your own small
unit fixtures to test each TODO. A count alone is not an equivalence proof.
