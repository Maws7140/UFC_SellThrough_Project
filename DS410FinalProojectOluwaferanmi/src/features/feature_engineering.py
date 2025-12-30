#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType, DoubleType
import argparse
import os


def make_fighter_stats(fights, stats, n=5):
    # Split into fighter1 and fighter2 views then union so each fighter gets their own row
    f1_selected = fights.select(
        "fight_id", "event_id", "event_date",
        F.col("fighter1_name").alias("fighter_name"),
        "winner_name", "method_category", "is_title_fight", "weight_class"
    )
    f1 = f1_selected.withColumn("won", (F.col("fighter_name") == F.col("winner_name")).cast(IntegerType()))

    f2_selected = fights.select(
        "fight_id", "event_id", "event_date",
        F.col("fighter2_name").alias("fighter_name"),
        "winner_name", "method_category", "is_title_fight", "weight_class"
    )
    f2 = f2_selected.withColumn("won", (F.col("fighter_name") == F.col("winner_name")).cast(IntegerType()))
    
    all_fights = f1.union(f2)
    
    # Check if fight ended in finish KO/TKO or Submission
    all_fights = all_fights.withColumn(
        "finished",
        F.when((F.col("won") == 1) & F.col("method_category").isin(["KO/TKO", "Submission"]), 1)
        .otherwise(0)
    )
    
    # Join with fight stats
    stats_selected = stats.select(
        "fight_id", "fighter_name",
        "sig_strikes_landed", "sig_strikes_attempted",
        "takedowns_landed", "takedowns_attempted"
    )
    all_fights = all_fights.join(stats_selected, on=["fight_id", "fighter_name"], how="left")

    # Window for all previous fights (career stats)
    win_all = Window.partitionBy("fighter_name").orderBy("event_date").rowsBetween(
        Window.unboundedPreceding, -1
    )

    # Window for last N fights
    win_n = Window.partitionBy("fighter_name").orderBy("event_date").rowsBetween(-n, -1)
    
    result = all_fights
    
    # Calculate rolling statistics
    result = result.withColumn(f"win_rate_last{n}", F.avg("won").over(win_n))
    result = result.withColumn(f"finish_rate_last{n}", F.avg("finished").over(win_n))
    result = result.withColumn("total_fights", F.count("*").over(win_all))
    
    # Days since last fight
    fighter_window = Window.partitionBy("fighter_name").orderBy("event_date")
    result = result.withColumn("last_fight", F.lag("event_date", 1).over(fighter_window))
    result = result.withColumn("days_off", F.datediff("event_date", "last_fight"))

    # Career accuracy percentages
    strike_accuracy = F.when(F.col("sig_strikes_attempted") > 0,
                             F.col("sig_strikes_landed") / F.col("sig_strikes_attempted"))
    result = result.withColumn("strike_acc", F.avg(strike_accuracy).over(win_all))

    takedown_accuracy = F.when(F.col("takedowns_attempted") > 0,
                               F.col("takedowns_landed") / F.col("takedowns_attempted"))
    result = result.withColumn("td_acc", F.avg(takedown_accuracy).over(win_all))

    # Count total title fights
    result = result.withColumn("title_fights", F.sum(F.col("is_title_fight").cast(IntegerType())).over(win_all))

    cols = [
        "fight_id", "event_id", "event_date", "fighter_name",
        f"win_rate_last{n}", f"finish_rate_last{n}",
        "total_fights", "days_off", "strike_acc", "td_acc", "title_fights", "weight_class"
    ]
    
    return result.select(cols)


def make_matchup_features(fights, fighter_feats, fighters):
    df = fights
    
    # Create unique pair ID so A, B and B, A are the same
    df = df.withColumn("pair1", F.least("fighter1_name", "fighter2_name"))
    df = df.withColumn("pair2", F.greatest("fighter1_name", "fighter2_name"))

    # Count how many times these two have fought before
    prev_fights = fights.select(
        F.least("fighter1_name", "fighter2_name").alias("pair1"),
        F.greatest("fighter1_name", "fighter2_name").alias("pair2"),
        F.col("event_date").alias("prev_event_date")
    )
    
    current_fights = df.select("fight_id", F.col("event_date").alias("curr_event_date"), "pair1", "pair2")
    prev_joined = prev_fights.join(current_fights, on=["pair1", "pair2"])
    prev_filtered = prev_joined.filter(F.col("prev_event_date") < F.col("curr_event_date"))
    prev_counts = prev_filtered.groupBy("fight_id").agg(F.count("*").alias("times_fought"))
    
    df = df.join(prev_counts, on="fight_id", how="left")
    
    # Fill nulls with 0 for fighters who haven't met before
    df = df.fillna({"times_fought": 0})
    df = df.withColumn("is_rematch", (F.col("times_fought") > 0).cast(IntegerType()))
    df = df.withColumn("is_rivalry", (F.col("times_fought") >= 2).cast(IntegerType()))
    
    f1_stats = fighters.select(
        F.col("fighter_name").alias("fighter1_name"),
        F.col("height_inches").alias("h1"),
        F.col("reach_inches").alias("r1"),
        F.col("dob").alias("dob1")
    )

    f2_stats = fighters.select(
        F.col("fighter_name").alias("fighter2_name"),
        F.col("height_inches").alias("h2"),
        F.col("reach_inches").alias("r2"),
        F.col("dob").alias("dob2")
    )

    df = df.join(f1_stats, on="fighter1_name", how="left")
    df = df.join(f2_stats, on="fighter2_name", how="left")
    
    # Calculate physical differences
    df = df.withColumn("reach_diff", F.abs(F.col("r1") - F.col("r2")))
    df = df.withColumn("height_diff", F.abs(F.col("h1") - F.col("h2")))
    
    # Age difference in years
    age1 = F.datediff("event_date", "dob1")
    age2 = F.datediff("event_date", "dob2")
    df = df.withColumn("age_diff", F.abs(age1 - age2) / 365.25)
    
    return df.select([
        "fight_id", "event_id", "fighter1_name", "fighter2_name",
        "is_rematch", "is_rivalry", "is_title_fight",
        "reach_diff", "height_diff", "age_diff"
    ])


def make_event_features(events, fights, matchups, fighter_feats):
    df = events
    
    # Count fights per event
    fight_counts = fights.groupBy("event_id").agg(
        F.count("*").alias("num_fights"),
        F.sum(F.col("is_title_fight").cast(IntegerType())).alias("num_title_fights"),
        F.max(F.col("is_title_fight").cast(IntegerType())).alias("has_title")
    )
    df = df.join(fight_counts, on="event_id", how="left")
    
    # Count rematches and rivalries
    rematch_counts = matchups.groupBy("event_id").agg(
        F.sum(F.col("is_rematch").cast(IntegerType())).alias("num_rematches"),
        F.max("is_rivalry").alias("has_rivalry")
    )
    df = df.join(rematch_counts, on="event_id", how="left")
    
    # Aggregate matchup physical features
    matchup_stats = matchups.groupBy("event_id").agg(
        F.avg("reach_diff").alias("avg_reach_diff"),
        F.avg("height_diff").alias("avg_height_diff"),
        F.avg("age_diff").alias("avg_age_diff")
    )
    df = df.join(matchup_stats, on="event_id", how="left")
    
    # Average fighter experience on the card
    fighter_avg = fighter_feats.groupBy("event_id").agg(
        F.avg("total_fights").alias("avg_exp"),
        F.avg("win_rate_last5").alias("avg_win_rate"),
        F.max("total_fights").alias("max_exp")
    )
    df = df.join(fighter_avg, on="event_id", how="left")
    
    # Date features
    df = df.withColumn("day_of_week", F.dayofweek("event_date"))
    df = df.withColumn("month", F.month("event_date"))
    df = df.withColumn("is_saturday", (F.col("day_of_week") == 7).cast(IntegerType()))
    
    # Days since last event of same type
    win_prev = Window.partitionBy("event_type").orderBy("event_date")
    prev_event_date = F.lag("event_date", 1).over(win_prev)
    df = df.withColumn("days_since_last", F.datediff("event_date", prev_event_date))
    
    # Location features
    df = df.withColumn("is_vegas", F.lower(F.col("city")).contains("las vegas").cast(IntegerType()))
    df = df.withColumn("is_usa", (F.col("country") == "USA").cast(IntegerType()))
    df = df.withColumn("is_ppv", (F.col("event_type") == "PPV").cast(IntegerType()))
    
    final_cols = [
        "event_id", "event_name", "event_date", "event_type",
        "city", "country", "year",
        "num_fights", "num_title_fights", "has_title",
        "num_rematches", "has_rivalry",
        "avg_exp", "avg_win_rate", "max_exp",
        "avg_reach_diff", "avg_height_diff", "avg_age_diff",
        "day_of_week", "month", "is_saturday", "days_since_last",
        "is_vegas", "is_usa", "is_ppv"
    ]
    
    return df.select(final_cols)


def load_external_features(spark, data_dir):
    external = {}
    external_path = os.path.join(data_dir, "external")
    features_path = os.path.join(data_dir, "features")
    
    # Load graph features
    graph_path = os.path.join(features_path, "graph_features")
    if os.path.exists(graph_path):
        external["graph"] = spark.read.parquet(graph_path)
    
    # Load betting odds
    betting_path = os.path.join(external_path, "betting_odds.csv")
    if os.path.exists(betting_path):
        betting = spark.read.csv(betting_path, header=True, inferSchema=True)
        for col in betting.columns:
            betting = betting.withColumnRenamed(col, col.lower())
        external["betting"] = betting
    
    # Load fighter buzz data
    buzz_path = os.path.join(external_path, "fighter_buzz.csv")
    if os.path.exists(buzz_path):
        buzz = spark.read.csv(buzz_path, header=True, inferSchema=True)
        for col in buzz.columns:
            buzz = buzz.withColumnRenamed(col, col.lower())
        external["buzz"] = buzz
    
    # Load sentiment data
    sentiment_path = os.path.join(external_path, "event_sentiment.csv")
    if os.path.exists(sentiment_path):
        sentiment = spark.read.csv(sentiment_path, header=True, inferSchema=True)
        for col in sentiment.columns:
            sentiment = sentiment.withColumnRenamed(col, col.lower())
        external["sentiment"] = sentiment
    
    return external


def make_graph_features(fights, graph_df):
    if graph_df is None:
        return fights

    # Drop existing columns to avoid conflicts
    cols_to_drop = [
        "f1_pagerank", "f1_network_size", "f1_community",
        "f2_pagerank", "f2_network_size", "f2_community",
        "combined_pagerank", "pagerank_differential", 
        "combined_network_size", "same_community"
    ]
    fights = fights.drop(*cols_to_drop)
    
    # Normalize names for joining (remove special chars, lowercase)
    graph_df = graph_df.withColumn(
        "name_norm",
        F.regexp_replace(F.lower(F.col("name")), r"[^a-z0-9]", "")
    )

    fights = fights.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    fights = fights.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )
    
    # Join graph features for fighter1
    graph_f1 = graph_df.select(
        F.col("name_norm").alias("f1_name_norm"),
        F.col("pagerank_score").alias("f1_pagerank"),
        F.col("num_opponents").alias("f1_network_size"),
        F.col("community_id").alias("f1_community")
    )
    fights = fights.join(graph_f1, on="f1_name_norm", how="left")
    
    # Join graph features for fighter2
    graph_f2 = graph_df.select(
        F.col("name_norm").alias("f2_name_norm"),
        F.col("pagerank_score").alias("f2_pagerank"),
        F.col("num_opponents").alias("f2_network_size"),
        F.col("community_id").alias("f2_community")
    )
    fights = fights.join(graph_f2, on="f2_name_norm", how="left")
    
    # Calculate combined features
    f1_pr = F.coalesce(F.col("f1_pagerank"), F.lit(0.0))
    f2_pr = F.coalesce(F.col("f2_pagerank"), F.lit(0.0))
    fights = fights.withColumn("combined_pagerank", f1_pr + f2_pr)
    fights = fights.withColumn("pagerank_differential", F.abs(f1_pr - f2_pr))
    
    f1_net = F.coalesce(F.col("f1_network_size"), F.lit(0))
    f2_net = F.coalesce(F.col("f2_network_size"), F.lit(0))
    fights = fights.withColumn("combined_network_size", f1_net + f2_net)
    
    fights = fights.withColumn("same_community", (F.col("f1_community") == F.col("f2_community")).cast(IntegerType()))
    
    return fights


def make_betting_features(fights, betting_df):
    if betting_df is None:
        return fights

    # Drop existing columns to avoid conflicts
    cols_to_drop = [
        "betting_spread", "is_competitive", "has_heavy_favorite",
        "f1_implied_prob", "f2_implied_prob"
    ]
    fights = fights.drop(*cols_to_drop)
    
    # Normalize names for joining
    betting_df = betting_df.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    betting_df = betting_df.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )

    fights = fights.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    fights = fights.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )
    
    # Select and prepare betting features
    betting_features = betting_df.select(
        "f1_name_norm", "f2_name_norm",
        F.col("odds_spread").alias("betting_spread"),
        F.col("is_competitive_matchup").cast(IntegerType()).alias("is_competitive"),
        F.col("has_heavy_favorite").cast(IntegerType()).alias("has_heavy_favorite"),
        F.col("fighter1_implied_prob").alias("f1_implied_prob"),
        F.col("fighter2_implied_prob").alias("f2_implied_prob")
    )
    betting_features = betting_features.dropDuplicates(["f1_name_norm", "f2_name_norm"])
    
    fights = fights.join(betting_features, on=["f1_name_norm", "f2_name_norm"], how="left")
    
    # Fill missing values with defaults
    fights = fights.fillna({
        "betting_spread": 0.1,
        "is_competitive": 1,
        "has_heavy_favorite": 0,
        "f1_implied_prob": 0.5,
        "f2_implied_prob": 0.5
    })
    
    return fights


def make_buzz_features(fights, buzz_df):
    if buzz_df is None:
        return fights

    # Drop existing columns to avoid conflicts
    cols_to_drop = [
        "pre_event_buzz_7d", "pre_event_buzz_30d",
        "peak_search_interest", "buzz_differential"
    ]
    fights = fights.drop(*cols_to_drop)
    
    # Normalize names
    buzz_df = buzz_df.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    buzz_df = buzz_df.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )

    fights = fights.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    fights = fights.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )
    
    # Select buzz features
    buzz_features = buzz_df.select(
        "f1_name_norm", "f2_name_norm",
        F.col("combined_buzz_7d").alias("pre_event_buzz_7d"),
        F.col("combined_buzz_30d").alias("pre_event_buzz_30d"),
        F.col("max_peak_search").alias("peak_search_interest"),
        F.col("buzz_differential").alias("buzz_differential")
    )
    buzz_features = buzz_features.dropDuplicates(["f1_name_norm", "f2_name_norm"])
    
    fights = fights.join(buzz_features, on=["f1_name_norm", "f2_name_norm"], how="left")
    
    # Fill missing values with defaults
    fights = fights.fillna({
        "pre_event_buzz_7d": 50.0,
        "pre_event_buzz_30d": 50.0,
        "peak_search_interest": 30.0,
        "buzz_differential": 0.0
    })
    
    return fights


def make_sentiment_features(events, sentiment_df):
    if sentiment_df is None:
        return events

    # Normalize event names for joining
    sentiment_df = sentiment_df.withColumn(
        "event_name_norm",
        F.regexp_replace(F.lower(F.col("event_name")), r"[^a-z0-9]", "")
    )

    events = events.withColumn(
        "event_name_norm",
        F.regexp_replace(F.lower(F.col("event_name")), r"[^a-z0-9]", "")
    )
    
    # Select sentiment features
    sentiment_features = sentiment_df.select(
        "event_name_norm",
        F.col("avg_sentiment").alias("reddit_sentiment"),
        F.col("hype_score").alias("reddit_hype"),
        F.col("total_engagement").alias("reddit_engagement"),
        F.col("comment_count").alias("reddit_comments")
    )
    sentiment_features = sentiment_features.dropDuplicates(["event_name_norm"])
    
    events = events.join(sentiment_features, on="event_name_norm", how="left")
    
    # Fill missing values with defaults
    events = events.fillna({
        "reddit_sentiment": 0.0,
        "reddit_hype": 0.5,
        "reddit_engagement": 100,
        "reddit_comments": 50
    })
    
    return events


def aggregate_external_to_event(fights_with_external, event_id_col="event_id"):
    agg_cols = []
    
    fight_cols = fights_with_external.columns
    
    if "combined_pagerank" in fight_cols:
        agg_cols.extend([
            F.avg("combined_pagerank").alias("avg_combined_pagerank"),
            F.max("combined_pagerank").alias("max_combined_pagerank"),
            F.avg("pagerank_differential").alias("avg_pagerank_diff")
        ])
    
    if "combined_network_size" in fight_cols:
        agg_cols.append(F.avg("combined_network_size").alias("avg_network_size"))
    
    if "same_community" in fight_cols:
        agg_cols.append(F.sum("same_community").alias("num_same_community_fights"))
    
    if "betting_spread" in fight_cols:
        agg_cols.extend([
            F.avg("betting_spread").alias("avg_betting_spread"),
            F.max("betting_spread").alias("max_betting_spread"),
            F.min("betting_spread").alias("min_betting_spread")
        ])
    
    if "is_competitive" in fight_cols:
        agg_cols.append(F.sum("is_competitive").alias("num_competitive_fights"))
    
    if "has_heavy_favorite" in fight_cols:
        agg_cols.append(F.sum("has_heavy_favorite").alias("num_heavy_favorites"))
    
    if "pre_event_buzz_7d" in fight_cols:
        agg_cols.extend([
            F.avg("pre_event_buzz_7d").alias("avg_buzz_7d"),
            F.max("pre_event_buzz_7d").alias("max_buzz_7d"),
            F.sum("pre_event_buzz_7d").alias("total_buzz_7d")
        ])
    
    if "buzz_differential" in fight_cols:
        agg_cols.append(F.avg("buzz_differential").alias("avg_buzz_diff"))
    
    if not agg_cols:
        return None
    
    result = fights_with_external.groupBy(event_id_col).agg(*agg_cols)
    
    return result


def make_extended_event_features(events, fights, matchups, fighter_feats, external_data):
    df = make_event_features(events, fights, matchups, fighter_feats)
    
    fights_enriched = fights
    
    if "graph" in external_data:
        fights_enriched = make_graph_features(fights_enriched, external_data["graph"])
    
    if "betting" in external_data:
        fights_enriched = make_betting_features(fights_enriched, external_data["betting"])
    
    if "buzz" in external_data:
        fights_enriched = make_buzz_features(fights_enriched, external_data["buzz"])
    
    # aggregate to event level
    external_agg = aggregate_external_to_event(fights_enriched)
    
    if external_agg is not None:
        df = df.join(external_agg, on="event_id", how="left")
    
    if "sentiment" in external_data:
        df = make_sentiment_features(df, external_data["sentiment"])
    
    df = df.fillna(0)
    
    return df


def run_all(data_dir, output_dir, use_external=True):
    print("Starting feature engineering...")

    builder = SparkSession.builder
    builder = builder.appName("UFC-Features")
    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.executor.memory", "4g")
    builder = builder.config("spark.sql.shuffle.partitions", "200")
    builder = builder.config("spark.default.parallelism", "200")

    # Only set master to local if not already set allows submission to cluster
    if "SPARK_MASTER" not in os.environ and "spark.master" not in str(builder._options):
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        proc_dir = os.path.join(data_dir, "processed")

        events = spark.read.parquet(os.path.join(proc_dir, "events"))
        fighters = spark.read.parquet(os.path.join(proc_dir, "fighters"))
        fights = spark.read.parquet(os.path.join(proc_dir, "fights"))
        stats = spark.read.parquet(os.path.join(proc_dir, "fight_stats"))

        events = events.cache()
        fighters = fighters.cache()
        fights = fights.cache()
        stats = stats.cache()

        print(f"Loaded and cached data")
        
        # Need event_date in fights for window functions
        event_dates = events.select("event_id", "event_date")
        fights = fights.join(event_dates, on="event_id", how="left")
        
        external_data = {}
        if use_external:
            external_data = load_external_features(spark, data_dir)
        
        fighter_feats = make_fighter_stats(fights, stats, n=5)
        matchup_feats = make_matchup_features(fights, fighter_feats, fighters)
        
        if external_data:
            event_feats = make_extended_event_features(
                events, fights, matchup_feats, fighter_feats, external_data
            )
        else:
            event_feats = make_event_features(events, fights, matchup_feats, fighter_feats)
        
        feat_dir = os.path.join(output_dir, "features")
        os.makedirs(feat_dir, exist_ok=True)
        
        fighter_feats.write.mode("overwrite").parquet(os.path.join(feat_dir, "fighter_features"))
        matchup_feats.write.mode("overwrite").parquet(os.path.join(feat_dir, "matchup_features"))
        event_feats.write.mode("overwrite").parquet(os.path.join(feat_dir, "event_features"))
        
        print("Feature engineering complete")
        return event_feats
        
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--output-dir", default="./data")
    parser.add_argument("--no-external", action="store_true", help="Skip external data features")
    
    args = parser.parse_args()
    
    run_all(args.data_dir, args.output_dir, use_external=not args.no_external)
