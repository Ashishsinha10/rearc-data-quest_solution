from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

BLS_RAW_PATH = "/Volumes/workspace/rearc/rearc_landing/raw/bls"
DATAUSA_RAW_PATH = "/Volumes/workspace/rearc/rearc_landing/raw/datausa"

BLS_DATA_FILE = f"{BLS_RAW_PATH}/pr.data.1.AllData"
BLS_SERIES_FILE = f"{BLS_RAW_PATH}/pr.series"
DATAUSA_POPULATION_FILE = f"{DATAUSA_RAW_PATH}/population.json"

VALID_BLS_PERIODS = ["Q01", "Q02", "Q03", "Q04", "Q05"]


# ============================================================
# COMMON HELPERS
# ============================================================

def normalize_headers(df, lowercase=False):
    """Trim source headers and optionally normalize them for Delta."""
    if lowercase:
        return df.toDF(*[
            c.strip().lower().replace(" ", "_").replace(".", "_")
            for c in df.columns
        ])

    return df.toDF(*[c.strip() for c in df.columns])


def validate_schema(df, required_columns, dataset_name):
    """
    Validate required column presence during pipeline planning.
    This is a schema guard, not a row-level Lakeflow expectation.
    """
    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"{dataset_name}: Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    return df


def trim_columns(df, columns):
    """Trim string values in the requested columns."""
    for column_name in columns:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                F.trim(F.col(column_name))
            )
    return df


def normalize_lookup(df, key_column, value_columns):
    """Normalize lookup keys and descriptive text before metadata joins."""
    df = normalize_headers(df)
    df = trim_columns(df, [key_column] + value_columns)
    return df


# ============================================================
# BRONZE LAYER - BLS SOURCE DATA
# ============================================================

@dp.materialized_view(name="bronze_bls_data")
@dp.expect_or_fail(
    "series_id_not_null",
    "series_id IS NOT NULL AND TRIM(series_id) <> ''"
)
@dp.expect_or_fail(
    "year_not_null",
    "year IS NOT NULL AND TRIM(year) <> ''"
)
@dp.expect_or_fail(
    "period_not_null",
    "period IS NOT NULL AND TRIM(period) <> ''"
)
@dp.expect_or_fail(
    "value_not_null",
    "value IS NOT NULL AND TRIM(value) <> ''"
)
def bronze_bls_data():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(BLS_DATA_FILE)
    )

    df = normalize_headers(df)

    return validate_schema(
        df,
        [
            "series_id",
            "year",
            "period",
            "value",
            "footnote_codes"
        ],
        "bronze_bls_data"
    )


@dp.materialized_view(name="bronze_bls_series")
@dp.expect_or_fail(
    "series_id_not_null",
    "series_id IS NOT NULL AND TRIM(series_id) <> ''"
)
def bronze_bls_series():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(BLS_SERIES_FILE)
    )

    df = normalize_headers(df)

    return validate_schema(
        df,
        [
            "series_id",
            "sector_code",
            "class_code",
            "measure_code",
            "duration_code",
            "seasonal"
        ],
        "bronze_bls_series"
    )


@dp.materialized_view(name="bronze_bls_sector")
def bronze_bls_sector():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.sector")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_measure")
def bronze_bls_measure():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.measure")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_period")
def bronze_bls_period():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.period")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_seasonal")
def bronze_bls_seasonal():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.seasonal")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_duration")
def bronze_bls_duration():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.duration")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_class")
def bronze_bls_class():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.class")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_footnote")
def bronze_bls_footnote():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.footnote")
    )
    return normalize_headers(df)


@dp.materialized_view(name="bronze_bls_contacts")
def bronze_bls_contacts():
    df = (
        spark.read
        .option("header", "true")
        .option("sep", "\t")
        .csv(f"{BLS_RAW_PATH}/pr.contacts")
    )

    return normalize_headers(df, lowercase=True)


# ============================================================
# BRONZE LAYER - DATAUSA
# ============================================================

@dp.materialized_view(name="bronze_datausa_population")
@dp.expect_or_fail(
    "year_not_null",
    "year IS NOT NULL"
)
@dp.expect_or_fail(
    "nation_not_null",
    "nation IS NOT NULL AND TRIM(nation) <> ''"
)
@dp.expect_or_fail(
    "population_not_null",
    "population IS NOT NULL"
)
def bronze_datausa_population():
    df = (
        spark.read
        .option("multiLine", "true")
        .json(DATAUSA_POPULATION_FILE)
        .select(F.explode("data").alias("record"))
        .select("record.*")
    )

    df = normalize_headers(df, lowercase=True)

    return validate_schema(
        df,
        ["year", "nation", "population"],
        "bronze_datausa_population"
    )


# ============================================================
# SILVER LAYER - BLS DATA
# ============================================================

@dp.materialized_view(name="silver_bls_data")
@dp.expect_or_fail(
    "series_id_not_null",
    "series_id IS NOT NULL AND TRIM(series_id) <> ''"
)
@dp.expect_or_fail(
    "year_valid",
    "year IS NOT NULL"
)
@dp.expect_or_fail(
    "period_valid",
    "period IN ('Q01', 'Q02', 'Q03', 'Q04', 'Q05')"
)
@dp.expect_or_fail(
    "value_valid",
    "value IS NOT NULL"
)
def silver_bls_data():
    df = spark.read.table("bronze_bls_data")
    df = normalize_headers(df)

    df = df.select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.expr("try_cast(year AS INT)").alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.expr("try_cast(value AS DOUBLE)").alias("value"),
        F.trim(F.col("footnote_codes")).alias("footnote_codes")
    )

    return validate_schema(
        df,
        [
            "series_id",
            "year",
            "period",
            "value",
            "footnote_codes"
        ],
        "silver_bls_data"
    )


# ============================================================
# DATA QUALITY GUARD - BLS OBSERVATION UNIQUENESS
# ============================================================

@dp.materialized_view(name="quality_bls_data_unique_keys")
@dp.expect_or_fail(
    "no_duplicate_observation_keys",
    "duplicate_key_count = 0"
)
def quality_bls_data_unique_keys():
    duplicate_keys = (
        spark.read.table("silver_bls_data")
        .groupBy("series_id", "year", "period")
        .count()
        .filter(F.col("count") > 1)
    )

    return duplicate_keys.agg(
        F.count("*").alias("duplicate_key_count")
    )


# ============================================================
# SILVER LAYER - DATAUSA POPULATION
# ============================================================

@dp.materialized_view(name="silver_datausa_population")
@dp.expect_or_fail(
    "year_valid",
    "year IS NOT NULL"
)
@dp.expect_or_fail(
    "population_valid",
    "population IS NOT NULL AND population > 0"
)
@dp.expect_or_fail(
    "nation_not_null",
    "nation IS NOT NULL AND TRIM(nation) <> ''"
)
def silver_datausa_population():
    df = (
        spark.read.table("bronze_datausa_population")
        .select(
            F.expr("try_cast(year AS INT)").alias("year"),
            F.trim(F.col("nation")).alias("nation"),
            F.expr("try_cast(population AS BIGINT)").alias("population")
        )
        .filter(F.col("nation") == "United States")
    )

    return validate_schema(
        df,
        ["year", "nation", "population"],
        "silver_datausa_population"
    )


# ============================================================
# SILVER LAYER - BLS SERIES METADATA
# ============================================================

@dp.materialized_view(name="silver_bls_series")
@dp.expect_or_fail(
    "series_id_not_null",
    "series_id IS NOT NULL AND TRIM(series_id) <> ''"
)
@dp.expect_or_fail(
    "series_label_complete",
    "series_label IS NOT NULL AND TRIM(series_label) <> ''"
)
def silver_bls_series():
    series = (
        spark.read.table("bronze_bls_series")
        .select(
            F.trim("series_id").alias("series_id"),
            F.trim("sector_code").alias("sector_code"),
            F.trim("class_code").alias("class_code"),
            F.trim("measure_code").alias("measure_code"),
            F.trim("duration_code").alias("duration_code"),
            F.trim("seasonal").alias("seasonal_code")
        )
    )

    sector = normalize_lookup(
        spark.read.table("bronze_bls_sector"),
        "sector_code",
        ["sector_name"]
    )
    class_df = normalize_lookup(
        spark.read.table("bronze_bls_class"),
        "class_code",
        ["class_text"]
    )
    measure = normalize_lookup(
        spark.read.table("bronze_bls_measure"),
        "measure_code",
        ["measure_text"]
    )
    duration = normalize_lookup(
        spark.read.table("bronze_bls_duration"),
        "duration_code",
        ["duration_text"]
    )
    seasonal = normalize_lookup(
        spark.read.table("bronze_bls_seasonal"),
        "seasonal_code",
        ["seasonal_text"]
    ).withColumnRenamed(
        "seasonal_code",
        "metadata_seasonal_code"
    )

    joined = (
        series
        .join(sector, "sector_code", "left")
        .join(class_df, "class_code", "left")
        .join(measure, "measure_code", "left")
        .join(duration, "duration_code", "left")
        .join(
            seasonal,
            F.col("seasonal_code") == F.col("metadata_seasonal_code"),
            "left"
        )
    )

    # concat_ws skips nulls. Require every metadata component so that
    # incomplete lookup joins produce a null label and fail the expectation.
    complete_metadata = (
        F.col("sector_name").isNotNull()
        & F.col("class_text").isNotNull()
        & F.col("measure_text").isNotNull()
        & F.col("duration_text").isNotNull()
        & F.col("seasonal_text").isNotNull()
    )

    result = joined.select(
        "series_id",
        F.when(
            complete_metadata,
            F.concat_ws(
                " | ",
                "sector_name",
                "class_text",
                "measure_text",
                "duration_text",
                "seasonal_text"
            )
        ).otherwise(F.lit(None).cast("string")).alias("series_label")
    ).filter(
        F.col("series_id").isNotNull()
        & (F.trim(F.col("series_id")) != "")
    )

    return validate_schema(
        result,
        ["series_id", "series_label"],
        "silver_bls_series"
    )


# ============================================================
# DATA QUALITY GUARD - SERIES METADATA UNIQUENESS
# ============================================================

@dp.materialized_view(name="quality_bls_series_unique_keys")
@dp.expect_or_fail(
    "no_duplicate_series_ids",
    "duplicate_series_count = 0"
)
def quality_bls_series_unique_keys():
    duplicate_series = (
        spark.read.table("silver_bls_series")
        .groupBy("series_id")
        .count()
        .filter(F.col("count") > 1)
    )

    return duplicate_series.agg(
        F.count("*").alias("duplicate_series_count")
    )


# ============================================================
# GOLD LAYER - QUESTION 1
# Mean and sample standard deviation of US population 2013-2018
# ============================================================

@dp.materialized_view(name="gold_q1_population_stats")
@dp.expect_or_fail(
    "exactly_six_years",
    "total_years = 6 AND start_year = 2013 AND end_year = 2018"
)
def gold_q1_population_stats():
    return (
        spark.read.table("silver_datausa_population")
        .filter(F.col("year").between(2013, 2018))
        .agg(
            F.countDistinct("year").alias("total_years"),
            F.min("year").alias("start_year"),
            F.max("year").alias("end_year"),
            F.avg("population").alias("mean_population"),
            F.stddev_samp("population").alias("stddev_sample")
        )
    )


# ============================================================
# GOLD LAYER - QUESTION 2
# Year with largest sum of quarterly values per series.
# Q05 annual averages are excluded. Ties choose the earliest year.
# ============================================================

@dp.materialized_view(name="gold_q2_productivity")
@dp.expect_or_fail(
    "series_label_not_null",
    "series_label IS NOT NULL AND TRIM(series_label) <> ''"
)
@dp.expect_or_fail(
    "quarterly_total_not_null",
    "quarterly_total IS NOT NULL"
)
def gold_q2_productivity():
    # Cross joins make the quality guard datasets explicit dependencies
    # of this Gold output. Their expectations fail the pipeline if
    # duplicate observation keys or duplicate series IDs are detected.
    observation_quality = spark.read.table(
        "quality_bls_data_unique_keys"
    )
    series_quality = spark.read.table(
        "quality_bls_series_unique_keys"
    )

    quarterly_totals = (
        spark.read.table("silver_bls_data")
        .filter(
            F.col("period").isin("Q01", "Q02", "Q03", "Q04")
        )
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("quarterly_total"))
    )

    window_spec = (
        Window.partitionBy("series_id")
        .orderBy(
            F.desc("quarterly_total"),
            F.asc("year")
        )
    )

    ranked = (
        quarterly_totals
        .withColumn(
            "rank",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rank") == 1)
        .drop("rank")
        .join(
            spark.read.table("silver_bls_series"),
            on="series_id",
            how="left"
        )
    )

    return (
        ranked
        .crossJoin(observation_quality)
        .crossJoin(series_quality)
        .select(
            "series_id",
            "series_label",
            "year",
            "quarterly_total"
        )
    )


# ============================================================
# GOLD LAYER - QUESTION 3
# BLS annual productivity value joined with US population
# ============================================================

@dp.materialized_view(name="gold_q3_bls_population")
@dp.expect_or_fail(
    "annual_value_not_null",
    "annual_value IS NOT NULL"
)
def gold_q3_bls_population():
    bls_data = (
        spark.read.table("silver_bls_data")
        .filter(
            (F.col("series_id") == "PRS30006032")
            & (F.col("period") == "Q01")
        )
        .select(
            "year",
            F.col("value").alias("annual_value")
        )
    )

    population = (
        spark.read.table("silver_datausa_population")
        .select(
            "year",
            "population"
        )
    )

    return (
        bls_data
        .join(
            population,
            on="year",
            how="left"
        )
        .select(
            "year",
            "annual_value",
            "population"
        )
    )
