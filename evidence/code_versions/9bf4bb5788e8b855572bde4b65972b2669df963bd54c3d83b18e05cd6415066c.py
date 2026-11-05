"""Complete these FIVE assessed functions. Do not edit the reference data.
Use the function signatures exactly: src/run_lab.py imports this module.
All functions operate on Spark DataFrames except validate_release_binding.
"""

def build_gold(silver_df):
    """TODO 1 (T2). Group by site and UTC event_date.
    Return exactly these columns:
      site, event_date, n_observations, sum_temperature_c,
      mean_temperature_c, min_temperature_c, max_temperature_c,
      n_late, late_rate, n_above_25, n_sensors.
    Use count of observations; SUM / COUNT for the mean; temperature > 25
    (strict inequality); COUNT(DISTINCT sensor_id); late flag as 0/1.
    Cast sums/min/max to decimal(18,2), mean/rate to decimal(18,6).
    No collect(), Pandas, Python UDF, or reference-sample join here.
    """
    raise NotImplementedError('TODO 1: build_gold')


def choose_revisions(changes_df):
    """TODO 2 (T3). Exactly one incoming row per record_id.
    Ordering: ingest_time_utc DESC, source_object ASC, source_row ASC.
    Use a deterministic Window and ROW_NUMBER. Return original columns;
    do not leak the helper rank column into a Delta table.
    """
    raise NotImplementedError('TODO 2: choose_revisions')


def merge_silver(spark, silver_path, winners_df):
    """TODO 3 (T3). Mutate an existing Delta table; return None.
    Match by record_id. Update only if the incoming row precedes the stored
    row in the SAME ordering as TODO 2; insert missing business keys.
    Equal provenance and ingest timestamp is a no-op. Stale data must not
    replace a newer value. The runner validates the WHOLE batch first.
    """
    raise NotImplementedError('TODO 3: merge_silver')


def benchmark_query(df):
    """TODO 4 (T4). Both layouts must evaluate exactly this query:
    filter event_date = DATE '2026-02-06'; group by site; return
    site, n_observations, sum_temperature_c (decimal(18,2)), n_late.
    Do not use replica_id in the GROUP BY. Do not call collect() here.
    The supplied runner materializes this bounded aggregate for timing.
    """
    raise NotImplementedError('TODO 4: benchmark_query')


def validate_release_binding(draft, expected):
    """TODO 5 (T5). Return (passed: bool, reasons: list[str]).
    Require nonempty author, reviewer, steward and reviewer != author.
    Bind handoff_logical_sha256, code_sha256, silver_path, silver_version,
    gold_path, gold_version and gold_source_silver_version to expected.
    Also require draft.gold_source_silver_version == draft.silver_version.
    Rejection reasons: REVIEW, METADATA, VERSION_BINDING, CODE_BINDING,
    or HANDOFF_BINDING. Missing keys must reject, never crash/pass.
    This is a course release check, not a digital signature or IAM control.
    """
    raise NotImplementedError('TODO 5: validate_release_binding')
