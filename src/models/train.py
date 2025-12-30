#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler
import argparse
import os
import json
from datetime import datetime


def create_spark_session():
    builder = SparkSession.builder
    builder = builder.appName("UFC-Model-Training")
    builder = builder.config("spark.driver.memory", "4g")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, data_dir):
    print("Loading data...")

    features_path = os.path.join(data_dir, "features", "event_features")
    if not os.path.exists(features_path):
        print(f"ERROR: Features not found")
        return None

    event_features = spark.read.parquet(features_path)

    attendance_path = os.path.join(data_dir, "external", "attendance.csv")
    if not os.path.exists(attendance_path):
        attendance_path = os.path.join(data_dir, "external", "attendance_full.csv")
    if not os.path.exists(attendance_path):
        attendance_path = os.path.join(data_dir, "external", "ufc_attendance_sample.csv")

    if not os.path.exists(attendance_path):
        print("ERROR: No attendance data found")
        return None

    attendance = spark.read.option("header", "true").csv(attendance_path)
    
    # Extract date from string might have extra text
    attendance = attendance.withColumn(
        "date_extracted",
        F.when(
            F.col("event_date").rlike(r"\d{4}-\d{2}-\d{2}"),
            F.regexp_extract(F.col("event_date"), r"(\d{4}-\d{2}-\d{2})", 1)
        ).otherwise(F.col("event_date"))
    )
    
    # Parse dates - try ISO format first, then long format
    iso_date = F.when(
        (F.col("date_extracted").isNotNull()) &
        (F.col("date_extracted") != "") &
        (F.col("date_extracted").rlike(r"^\d{4}-\d{2}-\d{2}$")),
        F.to_date("date_extracted", "yyyy-MM-dd")
    )
    long_date = F.when(
        F.col("date_extracted").rlike(r"^[A-Za-z]+ \d+, \d+"),
        F.to_date("date_extracted", "MMMM d, yyyy")
    )
    parsed_date = F.coalesce(iso_date, long_date, F.lit(None).cast("date"))
    
    # Parse numeric columns
    sell_through_parsed = F.when(
        (F.col("sell_through").isNotNull()) & (F.col("sell_through") != ""),
        F.col("sell_through").cast("double")
    ).otherwise(F.lit(None))
    
    attendance_parsed = F.when(
        (F.col("attendance").isNotNull()) & (F.col("attendance") != ""),
        F.col("attendance").cast("int")
    ).otherwise(F.lit(None))
    
    capacity_parsed = F.when(
        (F.col("venue_capacity").isNotNull()) & (F.col("venue_capacity") != ""),
        F.col("venue_capacity").cast("int")
    ).otherwise(F.lit(None))

    attendance = attendance.select(
        F.col("event_name"),
        parsed_date.alias("event_date"),
        sell_through_parsed.alias("sell_through"),
        attendance_parsed.alias("attendance"),
        capacity_parsed.alias("venue_capacity")
    ).filter(F.col("sell_through").isNotNull())
    
    def normalize_name(col):
        return F.regexp_replace(F.lower(col), r"[^a-z0-9]", "")

    def extract_ufc_number(col):
        return F.regexp_extract(col, r"UFC\s*(\d+)", 1)

    def extract_base_event_type(col):
        lower_col = F.lower(col)
        return F.coalesce(
            F.when(
                lower_col.rlike(r"ufc\s+(ultimate\s+)?fight\s+night"),
                F.lit("ufc fight night")
            ),
            F.when(
                lower_col.rlike(r"the\s+ultimate\s+fighter"),
                F.lit("the ultimate fighter")
            ),
            F.when(
                lower_col.rlike(r"ufc\s+on\s+\w+"),
                F.regexp_extract(lower_col, r"(ufc\s+on\s+\w+)", 1)
            ),
            F.when(
                lower_col.rlike(r"ufc\s+\d+"),
                F.lit("ufc")
            ),
        normalize_name(col)
    )

    event_features = event_features.withColumn("name_normalized", normalize_name("event_name"))
    event_features = event_features.withColumn("ufc_number", extract_ufc_number("event_name"))
    event_features = event_features.withColumn("base_event_type", extract_base_event_type("event_name"))

    attendance = attendance.withColumn("name_normalized", normalize_name("event_name"))
    attendance = attendance.withColumn("ufc_number", extract_ufc_number("event_name"))
    attendance = attendance.withColumn("base_event_type", extract_base_event_type("event_name"))

    # Join by name and date within 1 day
    attendance_selected = attendance.alias("att").select("name_normalized", "event_date", "sell_through")
    date_diff = F.datediff(F.col("ef.event_date"), F.col("att.event_date"))
    joined = event_features.alias("ef").join(
        attendance_selected,
        on=[
            F.col("ef.name_normalized") == F.col("att.name_normalized"),
            (date_diff >= -1) & (date_diff <= 1)
        ],
        how="inner"
    ).select(F.col("ef.*"), F.col("att.sell_through"))
    num_matched = joined.count()

    if num_matched < 400:
        attendance_distinct = attendance.select(F.col("event_date"), "sell_through").distinct()
        attendance_with_dates = attendance_distinct.withColumn(
            "match_dates",
            F.array(
                F.date_add(F.col("event_date"), -1),
                F.col("event_date"),
                F.date_add(F.col("event_date"), 1)
            )
        )
        attendance_dates_expanded = attendance_with_dates.select(
            F.explode("match_dates").alias("match_date"),
            F.col("event_date").alias("original_date"),
            "sell_through"
        )

        date_joined = event_features.join(
            attendance_dates_expanded,
            on=F.col("event_date") == F.col("match_date"),
            how="inner"
        ).drop("match_date", "original_date")

        num_matched_date = date_joined.count()

        if num_matched_date > num_matched:
            joined = date_joined.dropDuplicates(["event_id"])
            num_matched = joined.count()

    if num_matched < 450:
        fight_night_types = ["ufc fight night", "the ultimate fighter", "ufc on fox", "ufc on espn", "ufc on fuel tv", "ufc on fx"]

        event_features_special = event_features.filter(
            F.col("base_event_type").isin(fight_night_types)
        )

        attendance_special = attendance.filter(
            F.col("base_event_type").isin(fight_night_types)
        )

        attendance_special_distinct = attendance_special.select(
            F.col("base_event_type"),
            F.col("event_date"),
            "sell_through"
        ).distinct()
        attendance_special_with_dates = attendance_special_distinct.withColumn(
            "match_dates",
            F.array(
                F.date_add(F.col("event_date"), -1),
                F.col("event_date"),
                F.date_add(F.col("event_date"), 1)
            )
        )
        attendance_special_expanded = attendance_special_with_dates.select(
            F.col("base_event_type"),
            F.explode("match_dates").alias("match_date"),
            F.col("event_date").alias("original_date"),
            "sell_through"
        )

        special_joined = event_features_special.alias("ef").join(
            attendance_special_expanded.alias("att"),
            on=[
                F.col("ef.base_event_type") == F.col("att.base_event_type"),
                F.col("ef.event_date") == F.col("att.match_date")
            ],
            how="inner"
        ).select(
            F.col("ef.*"),
            F.col("att.sell_through")
        )

        num_matched_special = special_joined.count()

        if num_matched_special > 0:
            already_matched_ids = joined.select("event_id").distinct()
            new_special_matches = special_joined.join(
                already_matched_ids,
                on="event_id",
                how="left_anti"
            )

            if new_special_matches.count() > 0:
                joined = joined.union(new_special_matches.select(joined.columns))
                joined = joined.dropDuplicates(["event_id"])
                num_matched = joined.count()

    return joined


def prepare_features(df):
    base_features = [
        "num_fights", "num_title_fights", "has_title",
        "num_rematches", "has_rivalry", "avg_exp", "avg_win_rate",
        "max_exp", "day_of_week", "month", "is_saturday",
        "days_since_last", "is_vegas", "is_usa", "is_ppv"
    ]

    external_features = [
        "avg_combined_pagerank", "max_combined_pagerank", "avg_pagerank_diff",
        "avg_network_size", "num_same_community_fights",
        "avg_betting_spread", "max_betting_spread", "num_competitive_fights",
        "num_heavy_favorites", "avg_buzz_7d", "max_buzz_7d", "avg_buzz_diff",
        "reddit_sentiment", "reddit_hype", "reddit_engagement", "reddit_comments"
    ]

    feature_columns = base_features + external_features
    available = [c for c in feature_columns if c in df.columns]

    for col in available:
        df = df.fillna({col: 0})

    interaction_features = []

    if "has_title" in available and "is_vegas" in available:
        df = df.withColumn("title_vegas_interaction", F.col("has_title") * F.col("is_vegas"))
        interaction_features.append("title_vegas_interaction")

    if "is_ppv" in available and "avg_combined_pagerank" in available:
        df = df.withColumn("ppv_pagerank_interaction", F.col("is_ppv") * F.col("avg_combined_pagerank"))
        interaction_features.append("ppv_pagerank_interaction")

    if "num_competitive_fights" in available and "avg_buzz_7d" in available:
        df = df.withColumn("competitive_buzz_interaction", F.col("num_competitive_fights") * F.col("avg_buzz_7d"))
        interaction_features.append("competitive_buzz_interaction")

    if "has_title" in available and "reddit_hype" in available:
        df = df.withColumn("title_hype_interaction", F.col("has_title") * F.col("reddit_hype"))
        interaction_features.append("title_hype_interaction")

    if "num_rematches" in available and "is_usa" in available:
        df = df.withColumn("rematch_usa_interaction", F.col("num_rematches") * F.col("is_usa"))
        interaction_features.append("rematch_usa_interaction")

    available.extend(interaction_features)

    columns_to_keep = available + ["sell_through", "year", "event_date"]
    df = df.select(columns_to_keep)

    return df, available


def train_test_split(df, test_year=2024):
    if df.count() < 10:
        return df, df

    train = df.filter(F.col("year") < test_year)
    test = df.filter(F.col("year") >= test_year)

    if test.count() == 0 or train.count() == 0:
        train, test = df.randomSplit([0.8, 0.2], seed=42)

    return train, test


def train_model(train_df, feature_columns, do_cross_validation=True):
    print("Training model...")

    train_df.cache()
    train_count = train_df.count()

    if train_count < 50 and do_cross_validation:
        do_cross_validation = False

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="sell_through",
        maxDepth=6,
        maxIter=100,
        stepSize=0.05,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, gbt])
    
    if do_cross_validation:
        print("  Running cross-validation...")

        param_builder = ParamGridBuilder()
        param_builder = param_builder.addGrid(gbt.maxDepth, [5, 7])
        param_builder = param_builder.addGrid(gbt.maxIter, [50, 100])
        param_grid = param_builder.build()

        evaluator = RegressionEvaluator(
            labelCol="sell_through",
            predictionCol="prediction",
            metricName="rmse"
        )

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )

        model = cv.fit(train_df).bestModel
    else:
        model = pipeline.fit(train_df)

    return model


def evaluate_model(model, test_df):
    print("Evaluating model on test set...")

    test_count = test_df.count()
    print(f"  Test set size: {test_count}")

    if test_count == 0:
        return {"rmse": 0.0, "mae": 0.0, "r2": 0.0, "test_size": 0}

    predictions = model.transform(test_df)

    rmse_eval = RegressionEvaluator(labelCol="sell_through", predictionCol="prediction", metricName="rmse")
    mae_eval = RegressionEvaluator(labelCol="sell_through", predictionCol="prediction", metricName="mae")
    r2_eval = RegressionEvaluator(labelCol="sell_through", predictionCol="prediction", metricName="r2")

    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)

    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
    print(f"  R-squared: {r2:.4f}")

    print("\n  Sample predictions:")
    predictions.select("sell_through", "prediction").show(5)

    return {"rmse": rmse, "mae": mae, "r2": r2, "test_size": test_count}


def save_results(model, metrics, output_dir):
    models_dir = os.path.join(output_dir, "models")
    os.makedirs(models_dir, exist_ok=True)
    
    model_path = os.path.join(models_dir, "gbt_model")
    print(f"Saving model to {model_path}...")
    model.write().overwrite().save(model_path)
    
    metrics["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metrics_path = os.path.join(models_dir, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Saved metrics to {metrics_path}")


def main(data_dir, output_dir, test_year, use_cv):
    print("=" * 50)
    print("UFC Sell-Through Model Training")
    print("=" * 50)
    print(f"Data directory: {data_dir}")
    print(f"Test year: {test_year}")
    print(f"Cross-validation: {use_cv}")
    print()
    
    spark = create_spark_session()
    
    try:
        df = load_data(spark, data_dir)
        if df is None:
            return
        
        df, feature_cols = prepare_features(df)
        train_df, test_df = train_test_split(df, test_year)
        model = train_model(train_df, feature_cols, do_cross_validation=use_cv)
        metrics = evaluate_model(model, test_df)
        save_results(model, metrics, output_dir)
        
        print()
        print("=" * 50)
        print("TRAINING COMPLETE!")
        print("=" * 50)
        print(f"Features used: {len(feature_cols)}")
        print(f"Training events: {train_df.count()}")
        print(f"Test events: {test_df.count()}")
        print(f"Test RMSE: {metrics['rmse']:.4f}")
        print(f"Test R-squared: {metrics['r2']:.4f}")
        print("=" * 50)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train UFC sell-through prediction model")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    parser.add_argument("--test-year", type=int, default=2024, help="Year for test set")
    parser.add_argument("--no-cv", action="store_true", help="Skip cross-validation (faster)")
    
    args = parser.parse_args()
    
    main(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        test_year=args.test_year,
        use_cv=not args.no_cv
    )
