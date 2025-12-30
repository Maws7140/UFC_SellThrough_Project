from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler
import argparse
import os
import json
import pandas as pd
from datetime import datetime


def create_spark_session():
    builder = SparkSession.builder
    builder = builder.appName("UFC-Model-Training-Improved")
    builder = builder.config("spark.driver.memory", "6g")
    builder = builder.config("spark.sql.shuffle.partitions", "50")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, data_dir):
    print("Loading data...")

    features_path = os.path.join(data_dir, "features", "event_features")

    if not os.path.exists(features_path):
        print(f"ERROR: Features not found at {features_path}")
        return None

    event_features = spark.read.parquet(features_path)
    num_features = event_features.count()
    print(f"  Loaded {num_features} events with features")

    # Load attendance data
    attendance_path = os.path.join(data_dir, "external", "attendance.csv")
    if not os.path.exists(attendance_path):
        attendance_path = os.path.join(data_dir, "external", "attendance_full.csv")

    if not os.path.exists(attendance_path):
        print("ERROR: No attendance data found!")
        return None

    attendance = spark.read.option("header", "true").csv(attendance_path)
    num_attendance = attendance.count()
    print(f"  Loaded {num_attendance} events with attendance data")

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
            F.when(lower_col.rlike(r"ufc\s+(ultimate\s+)?fight\s+night"), F.lit("ufc fight night")),
            F.when(lower_col.rlike(r"the\s+ultimate\s+fighter"), F.lit("the ultimate fighter")),
            F.when(lower_col.rlike(r"ufc\s+on\s+\w+"), F.regexp_extract(lower_col, r"(ufc\s+on\s+\w+)", 1)),
            F.when(lower_col.rlike(r"ufc\s+\d+"), F.lit("ufc")),
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
    print(f"  Strategy 1 (name + date ±1 day): {num_matched} matches")

    if num_matched < 50:
        attendance_numbered = attendance.filter(F.col("ufc_number") != "").select(
            "ufc_number", "sell_through"
        ).dropDuplicates(["ufc_number"])

        joined_number = event_features.filter(F.col("ufc_number") != "").join(
            attendance_numbered, on="ufc_number", how="inner"
        )
        num_matched_number = joined_number.count()

        if num_matched_number > num_matched:
            joined = joined_number
            num_matched = num_matched_number

    if num_matched < 400:
        attendance_dates_expanded = attendance.select(
            F.col("event_date"), "sell_through"
        ).distinct().withColumn(
            "match_dates",
            F.array(
                F.date_add(F.col("event_date"), -1),
                F.col("event_date"),
                F.date_add(F.col("event_date"), 1)
            )
        ).select(
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

    print(f"\n  Final: {num_matched} events with sell_through data ({num_matched}/{num_features} = {num_matched/num_features*100:.1f}%)")

    return joined


def prepare_features(df):
    print("Preparing features...")

    feature_columns = [
        "num_fights",
        "num_title_fights",
        "has_title",
        "num_rematches",
        "has_rivalry",
        "avg_exp",
        "avg_win_rate",
        "max_exp",
        "avg_reach_diff",
        "avg_height_diff",
        "avg_age_diff",
        "day_of_week",
        "month",
        "is_saturday",
        "days_since_last",
        "is_vegas",
        "is_usa",
        "is_ppv",
        "avg_combined_pagerank",
        "max_combined_pagerank",
        "avg_pagerank_diff",
        "avg_network_size",
        "num_same_community_fights",
        "avg_betting_spread",
        "max_betting_spread",
        "min_betting_spread",
        "num_competitive_fights",
        "num_heavy_favorites",
        "avg_buzz_7d",
        "max_buzz_7d",
        "total_buzz_7d",
        "avg_buzz_diff",
        "reddit_sentiment",
        "reddit_hype",
        "reddit_engagement",
        "reddit_comments"
    ]

    available = [c for c in feature_columns if c in df.columns]
    missing = [c for c in feature_columns if c not in df.columns]

    if missing:
        print(f"  Note: These features are missing: {missing}")

    print(f"  Using {len(available)} features (vs 9 before): {available[:10]}...")

    for col in available:
        df = df.fillna({col: 0})

    columns_to_keep = available + ["sell_through", "year", "event_date", "event_name"]
    df = df.select(columns_to_keep)

    return df, available


def train_test_split(df, test_year=2024):
    print(f"Splitting data: training on <{test_year}, testing on >={test_year}")

    total_count = df.count()
    print(f"  Total rows before split: {total_count}")

    if total_count < 10:
        print(f"  WARNING: Only {total_count} rows total!")
        return df, df

    train = df.filter(F.col("year") < test_year)
    test = df.filter(F.col("year") >= test_year)

    train_count = train.count()
    test_count = test.count()

    print(f"  Training set: {train_count} events")
    print(f"  Test set: {test_count} events")

    if test_count == 0 or train_count == 0:
        print("  WARNING: Using 80/20 random split instead")
        train, test = df.randomSplit([0.8, 0.2], seed=42)

    return train, test


def train_model(train_df, feature_columns, do_cross_validation=True, model_type="gbt"):
    print(f"Training {model_type.upper()} model...")

    train_df.cache()
    train_count = train_df.count()
    print(f"  Cached training data: {train_count} rows")

    if train_count < 50 and do_cross_validation:
        print(f"  WARNING: Small dataset. Disabling CV.")
        do_cross_validation = False

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    if model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol="sell_through",
            maxDepth=7,
            maxIter=100,
            stepSize=0.1,
            seed=42
        )
    else:  # random forest
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="sell_through",
            numTrees=100,
            maxDepth=10,
            seed=42
        )

    pipeline = Pipeline(stages=[assembler, model])

    if do_cross_validation:
        print("  Running cross-validation...")
        print("  (Reduced hyperparameter grid for faster training)")

        if model_type == "gbt":
            param_builder = ParamGridBuilder()
            param_builder = param_builder.addGrid(model.maxDepth, [7, 10, 12])
            param_builder = param_builder.addGrid(model.maxIter, [100, 150])
            param_builder = param_builder.addGrid(model.stepSize, [0.05, 0.1])
            param_grid = param_builder.build()
        else:
            param_builder = ParamGridBuilder()
            param_builder = param_builder.addGrid(model.numTrees, [100, 150])
            param_builder = param_builder.addGrid(model.maxDepth, [10, 12])
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

        cv_model = cv.fit(train_df)
        fitted_model = cv_model.bestModel

        print("  Cross-validation complete!")

    else:
        print("  Training without cross-validation...")
        fitted_model = pipeline.fit(train_df)

    return fitted_model


def evaluate_model(model, test_df, feature_columns, output_dir):
    print("Evaluating model on test set...")

    test_count = test_df.count()
    print(f"  Test set size: {test_count}")

    if test_count == 0:
        return {"rmse": 0.0, "mae": 0.0, "r2": 0.0, "test_size": 0}

    predictions = model.transform(test_df)
    pred_count = predictions.count()
    print(f"  Predictions generated: {pred_count}")

    rmse_eval = RegressionEvaluator(
        labelCol="sell_through", 
        predictionCol="prediction", 
        metricName="rmse"
    )
    mae_eval = RegressionEvaluator(
        labelCol="sell_through", 
        predictionCol="prediction", 
        metricName="mae"
    )
    r2_eval = RegressionEvaluator(
        labelCol="sell_through", 
        predictionCol="prediction", 
        metricName="r2"
    )

    rmse = rmse_eval.evaluate(predictions)
    mae = mae_eval.evaluate(predictions)
    r2 = r2_eval.evaluate(predictions)

    mse = rmse ** 2

    print(f"\n  {'='*50}")
    print(f"  MODEL PERFORMANCE")
    print(f"  {'='*50}")
    print(f"  RMSE: {rmse:.4f}")
    print(f"  MAE: {mae:.4f}")
    print(f"  R-squared: {r2:.4f}")
    print(f"  MSE: {mse:.4f}")
    print(f"  {'='*50}\n")

    print("  Sample predictions:")
    predictions.select("event_name", "sell_through", "prediction").show(10, truncate=False)
    
    # Save predictions for visualization
    pred_dir = os.path.join(output_dir, "models")
    os.makedirs(pred_dir, exist_ok=True)
    pred_pandas = predictions.select("event_name", "sell_through", "prediction").toPandas()
    pred_pandas.to_csv(os.path.join(pred_dir, "predictions.csv"), index=False)
    print(f"  Saved predictions to {os.path.join(pred_dir, 'predictions.csv')}")

    try:
        gbt_model = model.stages[-1]
        if hasattr(gbt_model, 'featureImportances'):
            importances = gbt_model.featureImportances.toArray()
            feature_importance = list(zip(feature_columns, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)

            print("\n  Top 10 Most Important Features:")
            print(f"  {'='*60}")
            for i, (feat, imp) in enumerate(feature_importance[:10], 1):
                print(f"  {i:2d}. {feat:30s} {imp:.4f}")
            print(f"  {'='*60}\n")

            return {
                "rmse": rmse,
                "mae": mae,
                "r2": r2,
                "mse": mse,
                "test_size": test_count,
                "feature_importance": feature_importance
            }
    except:
        pass

    return {
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "mse": mse,
        "test_size": test_count
    }


def save_results(model, metrics, output_dir, feature_columns):
    models_dir = os.path.join(output_dir, "models")
    os.makedirs(models_dir, exist_ok=True)

    model_path = os.path.join(models_dir, "gbt_model_improved")
    print(f"Saving improved model to {model_path}...")
    model.write().overwrite().save(model_path)

    metrics_copy = metrics.copy()
    metrics_copy["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metrics_copy["num_features"] = len(feature_columns)

    if "feature_importance" in metrics_copy:
        metrics_copy["feature_importance"] = [
            {"feature": f, "importance": float(i)}
            for f, i in metrics_copy["feature_importance"]
        ]

    metrics_path = os.path.join(models_dir, "metrics_improved.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics_copy, f, indent=2)
    print(f"Saved metrics to {metrics_path}")

    features_path = os.path.join(models_dir, "features_used.txt")
    with open(features_path, "w") as f:
        f.write("\n".join(feature_columns))
    print(f"Saved feature list to {features_path}")


def main(data_dir, output_dir, test_year, use_cv, model_type="gbt"):
    print("=" * 60)
    print("UFC Sell-Through Model Training - IMPROVED VERSION")
    print("=" * 60)
    print(f"Data directory: {data_dir}")
    print(f"Test year: {test_year}")
    print(f"Cross-validation: {use_cv}")
    print(f"Model type: {model_type.upper()}")
    print()

    spark = create_spark_session()

    try:
        df = load_data(spark, data_dir)
        if df is None:
            return

        df, feature_cols = prepare_features(df)
        train_df, test_df = train_test_split(df, test_year)

        model = train_model(train_df, feature_cols, do_cross_validation=use_cv, model_type=model_type)
        metrics = evaluate_model(model, test_df, feature_cols, output_dir)

        save_results(model, metrics, output_dir, feature_cols)

        print()
        print("=" * 60)
        print("TRAINING COMPLETE - IMPROVEMENT SUMMARY")
        print("=" * 60)
        print(f"Features used: {len(feature_cols)} (vs 9 before = {(len(feature_cols)/9-1)*100:.0f}% increase)")
        print(f"Training events: {train_df.count()}")
        print(f"Test events: {metrics.get('test_size', 'N/A')}")
        print(f"Test RMSE: {metrics['rmse']:.4f}")
        print(f"Test R-squared: {metrics['r2']:.4f}")

        print("\nExpected improvement over baseline (R²=0.26):")
        if metrics['r2'] > 0.26:
            improvement = ((metrics['r2'] - 0.26) / 0.26) * 100
            print(f"  ✅ {improvement:+.1f}% improvement in R-squared!")
        else:
            print(f"  ⚠️  R-squared not improved (might need more tuning)")

        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train IMPROVED UFC sell-through prediction model")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    parser.add_argument("--test-year", type=int, default=2024, help="Year for test set")
    parser.add_argument("--no-cv", action="store_true", help="Skip cross-validation")
    parser.add_argument("--model", choices=["gbt", "rf"], default="gbt", help="Model type (gbt or rf)")

    args = parser.parse_args()

    main(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        test_year=args.test_year,
        use_cv=not args.no_cv,
        model_type=args.model
    )
