"""Complete these FIVE assessed functions. Do not edit the reference data.
Use the function signatures exactly: src/run_lab.py imports this module.
All functions operate on Spark DataFrames except validate_release_binding.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

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
    gold_df = (
        silver_df
        .groupBy("site", "event_date")
        .agg(
            F.count("*").alias("n_observations"),

            F.sum("temperature_c")
             .cast("decimal(18,2)")
             .alias("sum_temperature_c"),

            F.min("temperature_c")
             .cast("decimal(18,2)")
             .alias("min_temperature_c"),

            F.max("temperature_c")
             .cast("decimal(18,2)")
             .alias("max_temperature_c"),

            F.sum(
                F.when(F.col("is_late"), 1).otherwise(0)
            ).alias("n_late"),

            F.sum(
                F.when(F.col("temperature_c") > 25, 1)
                 .otherwise(0)
            ).alias("n_above_25"),

            F.countDistinct("sensor_id").alias("n_sensors")
        )
        .withColumn(
            "mean_temperature_c",
            (
                F.col("sum_temperature_c") /
                F.col("n_observations")
            ).cast("decimal(18,6)")
        )
        .withColumn(
            "late_rate",
            (
                F.col("n_late") /
                F.col("n_observations")
            ).cast("decimal(18,6)")
        )
        .select(
            "site",
            "event_date",
            "n_observations",
            "sum_temperature_c",
            "mean_temperature_c",
            "min_temperature_c",
            "max_temperature_c",
            "n_late",
            "late_rate",
            "n_above_25",
            "n_sensors"
        )
    )

    return gold_df


def choose_revisions(changes_df):
    """TODO 2 (T3). Exactly one incoming row per record_id.
    Ordering: ingest_time_utc DESC, source_object ASC, source_row ASC.
    Use a deterministic Window and ROW_NUMBER. Return original columns;
    do not leak the helper rank column into a Delta table.
    """
    window_spec = Window.partitionBy("record_id").orderBy(
        F.col("ingest_time_utc").desc(),
        F.col("source_object").asc(),
        F.col("source_row").cast("int").asc()
    )

    df_with_date = changes_df
    if "event_date" not in df_with_date.columns:
        df_with_date = df_with_date.withColumn("event_date", F.to_date(F.col("event_time_utc")))

    original_cols = df_with_date.columns

    winners_df = (
        df_with_date
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .select(*original_cols)
    )

    return winners_df


def merge_silver(spark, silver_path, winners_df):
    """TODO 3 (T3). Mutate an existing Delta table; return None.
    Match by record_id. Update only if the incoming row precedes the stored
    row in the SAME ordering as TODO 2; insert missing business keys.
    Equal provenance and ingest timestamp is a no-op. Stale data must not
    replace a newer value. The runner validates the WHOLE batch first.
    """
    silver_table = DeltaTable.forPath(spark, silver_path)
    update_condition = (
        "(source.ingest_time_utc > target.ingest_time_utc) OR "
        "(source.ingest_time_utc = target.ingest_time_utc AND source.source_object < target.source_object) OR "
        "(source.ingest_time_utc = target.ingest_time_utc AND source.source_object = target.source_object AND source.source_row < target.source_row)"
    )
    (
        silver_table.alias("target")
        .merge(
            winners_df.alias("source"),
            "target.record_id = source.record_id"
        )
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def benchmark_query(df):
    """TODO 4 (T4). Both layouts must evaluate exactly this query:
    filter event_date = DATE '2026-02-06'; group by site; return
    site, n_observations, sum_temperature_c (decimal(18,2)), n_late.
    Do not use replica_id in the GROUP BY. Do not call collect() here.
    The supplied runner materializes this bounded aggregate for timing.
    """
    result_df = (
        df
        .filter(
            F.col("event_date") == F.lit("2026-02-06").cast("date")
        )
        .groupBy("site")
        .agg(
            F.count("*").alias("n_observations"),

            F.sum("temperature_c")
             .cast("decimal(18,2)")
             .alias("sum_temperature_c"),

            F.sum(
                F.when(F.col("is_late"), 1).otherwise(0)
            ).alias("n_late")
        )
        .select(
            "site",
            "n_observations",
            "sum_temperature_c",
            "n_late"
        )
    )

    return result_df


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
