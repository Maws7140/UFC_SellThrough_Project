from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, FloatType, BooleanType
import argparse
import os


def make_spark(app_name="UFC-ETL", local=True):
    builder = SparkSession.builder
    builder = builder.appName(app_name)

    if local:
        builder = builder.master("local[*]")

    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.parquet.compression.codec", "snappy")
    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.executor.memory", "4g")
    builder = builder.config("spark.sql.shuffle.partitions", "8")
    builder = builder.config("spark.default.parallelism", "8")
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "10485760")

    return builder.getOrCreate()


def load_data(spark, data_dir):
    print("Loading CSV files...")
    
    raw_path = os.path.join(data_dir, "raw")
    
    file_map = {
        "events": "events.csv",
        "fight_results": "fight_results.csv",
        "fight_stats": "fight_stats.csv",
        "fight_details": "fight_details.csv",
        "fighter_details": "fighter_details.csv",
        "fighter_tott": "fighter_tott.csv",
    }
    
    data = {}
    for name, fname in file_map.items():
        full_path = os.path.join(raw_path, fname)
        if os.path.exists(full_path):
            df = spark.read.csv(full_path, header=True, inferSchema=True)
            # Make columns lowercase
            for col_name in df.columns:
                df = df.withColumnRenamed(col_name, col_name.lower())
            print(f"  Loaded {name}")
            data[name] = df
        else:
            print(f"  Warning: {full_path} not found")
    
    return data


def load_external_data(spark, data_dir):
    print("Loading external data sources...")
    
    external_path = os.path.join(data_dir, "external")
    external_data = {}
    
    external_files = {
        "betting_odds": "betting_odds.csv",
        "google_trends": "google_trends.csv",
        "fighter_buzz": "fighter_buzz.csv",
        "event_sentiment": "event_sentiment.csv",
        "reddit_comments": "reddit_comments.csv",
    }
    
    for name, fname in external_files.items():
        full_path = os.path.join(external_path, fname)
        if os.path.exists(full_path):
            df = spark.read.csv(full_path, header=True, inferSchema=True)
            for col_name in df.columns:
                df = df.withColumnRenamed(col_name, col_name.lower())
            print(f"  Loaded {name}")
            external_data[name] = df
        else:
            print(f"  Note: {fname} not found (run scrapers first)")
    
    return external_data


def load_graph_features(spark, data_dir):
    print("Loading graph features...")
    
    graph_path = os.path.join(data_dir, "features", "graph_features")
    
    if os.path.exists(graph_path):
        df = spark.read.parquet(graph_path)
        print(f"  Loaded graph features: {df.count()} fighters")
        return df
    else:
        print("  Note: Graph features not found (run fighter_network.py first)")
        return None


def standardize_date(date_col):
    # Extract embedded ISO date if present
    extracted = F.when(
        date_col.rlike(r"\d{4}-\d{2}-\d{2}"),
        F.regexp_extract(date_col, r"(\d{4}-\d{2}-\d{2})", 1)
    ).otherwise(date_col)
    
    # Try different date formats
    iso_date = F.when(extracted.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(extracted, "yyyy-MM-dd"))
    long_date = F.when(extracted.rlike(r"^[A-Za-z]+ \d+, \d+"), F.to_date(extracted, "MMMM d, yyyy"))
    short_date = F.when(extracted.rlike(r"^[A-Za-z]{3} \d+, \d+"), F.to_date(extracted, "MMM d, yyyy"))
    
    return F.coalesce(iso_date, long_date, short_date)


def clean_events(df):
    print("Cleaning events...")
    
    date_col = F.col("date")
    
    df = df.withColumn(
        "date_extracted",
        F.when(
            date_col.rlike(r"\d{4}-\d{2}-\d{2}"),
            F.regexp_extract(date_col, r"(\d{4}-\d{2}-\d{2})", 1)
        ).otherwise(date_col)
    )
    
    # Try different date formats
    iso_format = F.when(
        F.col("date_extracted").rlike(r"^\d{4}-\d{2}-\d{2}$"),
        F.to_date(F.col("date_extracted"), "yyyy-MM-dd")
    )
    long_format = F.when(
        F.col("date_extracted").rlike(r"^[A-Za-z]+ \d+, \d+"),
        F.to_date(F.col("date_extracted"), "MMMM d, yyyy")
    )
    df = df.withColumn("event_date", F.coalesce(iso_format, long_format))
    
    df = df.withColumn(
        "event_type",
        F.when(F.col("event").rlike("^UFC [0-9]+"), "PPV")
         .when(F.col("event").contains("Fight Night"), "Fight Night")
         .when(F.col("event").contains("TUF"), "TUF")
         .when(F.col("event").contains("Contender"), "DWCS")
         .otherwise("Other")
    )
    
    df = df.withColumn("location_parts", F.split(F.col("location"), ", "))
    df = df.withColumn("city", F.element_at(F.col("location_parts"), 1))
    df = df.withColumn("state_country", F.element_at(F.col("location_parts"), 2))
    
    # Standardize country names
    df = df.withColumn(
        "country",
        F.when(F.col("state_country").isin(["USA", "United States"]), "USA")
         .when(F.col("state_country").isin(["UK", "England", "Scotland"]), "UK")
         .when(F.col("state_country").isin(["Canada"]), "Canada")
         .otherwise(F.col("state_country"))
    )
    
    # Handle both event/ and event-details/ formats
    event_details_id = F.when(
        F.col("url").contains("event-details/"),
        F.regexp_extract(F.col("url"), r"event-details/([^/]+)", 1)
    )
    event_id_extracted = F.when(
        F.col("url").contains("event/"),
        F.regexp_extract(F.col("url"), r"event/([^/]+)", 1)
    )
    df = df.withColumn("event_id", F.coalesce(event_details_id, event_id_extracted, F.col("url")))
    
    df = df.withColumn("year", F.year(F.col("event_date")))
    
    df = df.select(
        F.col("event_id"),
        F.col("event").alias("event_name"),
        F.col("event_date"),
        F.col("event_type"),
        F.col("city"),
        F.col("country"),
        F.col("location"),
        F.col("year")
    )
    
    return df.dropDuplicates(["event_id"])


def clean_fighters(df):
    print("Cleaning fighters...")
    print(f"  Input columns: {df.columns}")
    
    # Handle both fighter/ and fighter-details/ formats
    fighter_details_id = F.when(
        F.col("url").contains("fighter-details/"),
        F.regexp_extract(F.col("url"), r"fighter-details/([^/]+)", 1)
    )
    fighter_id_extracted = F.when(
        F.col("url").contains("fighter/"),
        F.regexp_extract(F.col("url"), r"fighter/([^/]+)", 1)
    )
    df = df.withColumn("fighter_id", F.coalesce(fighter_details_id, fighter_id_extracted, F.col("url")))
    
    # helper to safely extract and cast numeric values
    def safe_extract_int(col_name, pattern):
        extracted = F.regexp_extract(F.col(col_name), pattern, 1)
        return F.when(F.length(extracted) > 0, extracted.cast(IntegerType())).otherwise(F.lit(None))
    
    if "height" in df.columns:
        feet = safe_extract_int("height", r"(\d+)'")
        inches = safe_extract_int("height", r"(\d+)\"")
        # Convert to total inches
        both_parts = F.when(feet.isNotNull() & inches.isNotNull(), feet * 12 + inches)
        feet_only = F.when(feet.isNotNull(), feet * 12)
        df = df.withColumn("height_inches", F.coalesce(both_parts, feet_only, F.lit(None).cast(IntegerType())))
    else:
        df = df.withColumn("height_inches", F.lit(None).cast(IntegerType()))
    
    if "reach" in df.columns:
        df = df.withColumn(
            "reach_inches",
            safe_extract_int("reach", r"(\d+)")
        )
    else:
        df = df.withColumn("reach_inches", F.lit(None).cast(IntegerType()))
    
    if "weight" in df.columns:
        df = df.withColumn(
            "weight_lbs",
            safe_extract_int("weight", r"(\d+)")
        )
    else:
        df = df.withColumn("weight_lbs", F.lit(None).cast(IntegerType()))
    
    if "dob" in df.columns:
        dob_col = F.col("dob")
        df = df.withColumn(
            "dob",
            F.when(
                (dob_col.isNotNull()) & 
                (dob_col != "") & 
                (dob_col != "--") &
                (dob_col.rlike(r"^[A-Za-z]+ \d+, \d+")),
                F.to_date(dob_col, "MMM dd, yyyy")
            ).otherwise(F.lit(None).cast("date"))
        )
    else:
        df = df.withColumn("dob", F.lit(None).cast("date"))
    
    if "record" in [c.lower() for c in df.columns]:
        wins_extracted = F.regexp_extract(F.col("record"), r"^(\d+)", 1)
        losses_extracted = F.regexp_extract(F.col("record"), r"^(\d+)-(\d+)", 2)
        draws_extracted = F.regexp_extract(F.col("record"), r"^(\d+)-(\d+)-(\d+)", 3)
        df = df.withColumn(
            "wins",
            F.when(F.length(wins_extracted) > 0, wins_extracted.cast(IntegerType())).otherwise(F.lit(None))
        ).withColumn(
            "losses",
            F.when(F.length(losses_extracted) > 0, losses_extracted.cast(IntegerType())).otherwise(F.lit(None))
        ).withColumn(
            "draws",
            F.when(F.length(draws_extracted) > 0, draws_extracted.cast(IntegerType())).otherwise(F.lit(0))
        )
    else:
        df = df.withColumn("wins", F.lit(None).cast(IntegerType()))
        df = df.withColumn("losses", F.lit(None).cast(IntegerType()))
        df = df.withColumn("draws", F.lit(None).cast(IntegerType()))
    
    if "fighter" in df.columns:
        df = df.withColumn("fighter_name", F.col("fighter"))
    elif "first" in df.columns and "last" in df.columns:
        df = df.withColumn("fighter_name", F.concat_ws(" ", F.col("first"), F.col("last")))
    else:
        df = df.withColumn("fighter_name", F.lit("Unknown"))
    
    if "nickname" in df.columns:
        df = df.withColumn("nickname", F.col("nickname"))
    else:
        df = df.withColumn("nickname", F.lit(None).cast("string"))
    
    if "stance" in df.columns:
        df = df.withColumn("stance", F.col("stance"))
    else:
        df = df.withColumn("stance", F.lit(None).cast("string"))
    
    df = df.select(
        "fighter_id",
        "fighter_name",
        "nickname",
        "height_inches",
        "weight_lbs",
        "reach_inches",
        "stance",
        "dob",
        "wins",
        "losses",
        "draws"
    )
    
    return df.dropDuplicates(["fighter_id"])


def clean_fights(df):
    print("Cleaning fights...")
    
    # handle both fight/ and fight-details/ formats
    df = df.withColumn(
        "fight_id",
        F.coalesce(
            F.when(
                F.col("url").contains("fight-details/"),
                F.regexp_extract(F.col("url"), r"fight-details/([^/]+)", 1)
            ),
            F.when(
                F.col("url").contains("fight/"),
                F.regexp_extract(F.col("url"), r"fight/([^/]+)", 1)
            ),
            F.col("url")
        )
    )
    
    if "event_url" in df.columns:
        df = df.withColumn(
            "event_id",
            F.coalesce(
                F.when(
                    F.col("event_url").contains("event-details/"),
                    F.regexp_extract(F.col("event_url"), r"event-details/([^/]+)", 1)
                ),
                F.when(
                    F.col("event_url").contains("event/"),
                    F.regexp_extract(F.col("event_url"), r"event/([^/]+)", 1)
                ),
                F.col("event_url")
            )
        )
    else:
        df = df.withColumn("event_id", F.lit(None).cast("string"))
    
    wc_col = "weightclass" if "weightclass" in df.columns else "weight_class"
    if wc_col in df.columns:
        df = df.withColumn(
            "is_title_fight",
            F.col(wc_col).contains("Title").cast(BooleanType())
        )
        df = df.withColumn(
            "weight_class_clean",
            F.regexp_replace(F.col(wc_col), " Title Bout", "")
        )
    else:
        df = df.withColumn("is_title_fight", F.lit(False))
        df = df.withColumn("weight_class_clean", F.lit("Unknown"))
    
    if "method" in df.columns:
        df = df.withColumn(
            "method_category",
            F.when(F.col("method").contains("KO"), "KO/TKO")
             .when(F.col("method").contains("SUB"), "Submission")
             .when(F.col("method").contains("DEC"), "Decision")
             .otherwise("Other")
        )
    else:
        df = df.withColumn("method", F.lit(None).cast("string"))
        df = df.withColumn("method_category", F.lit("Unknown"))
    
    if "round" in df.columns:
        round_extracted = F.regexp_extract(F.col("round"), r"(\d+)", 1)
        df = df.withColumn(
            "round_num",
            F.when(F.length(round_extracted) > 0, round_extracted.cast(IntegerType())).otherwise(F.lit(None))
        )
    else:
        df = df.withColumn("round_num", F.lit(None).cast(IntegerType()))
    
    if "bout" in df.columns:
        df = df.withColumn(
            "fighter1_name",
            F.trim(F.regexp_extract(F.col("bout"), r"^(.+?)\s+vs\.?\s+", 1))
        )
        df = df.withColumn(
            "fighter2_name",
            F.trim(F.regexp_extract(F.col("bout"), r"\s+vs\.?\s+(.+)$", 1))
        )
    elif "fighter1" in df.columns and "fighter2" in df.columns:
        df = df.withColumn("fighter1_name", F.col("fighter1"))
        df = df.withColumn("fighter2_name", F.col("fighter2"))
    else:
        df = df.withColumn("fighter1_name", F.lit("Unknown"))
        df = df.withColumn("fighter2_name", F.lit("Unknown"))
    
    if "winner" in df.columns:
        df = df.withColumn("winner_name", F.col("winner"))
    elif "outcome" in df.columns:
        df = df.withColumn(
            "winner_name",
            F.when(F.col("outcome").startswith("W"), F.col("fighter1_name"))
             .when(F.col("outcome").startswith("L"), F.col("fighter2_name"))
             .otherwise(F.lit(None).cast("string"))
        )
    else:
        df = df.withColumn("winner_name", F.lit(None).cast("string"))
    
    if "time" in df.columns:
        df = df.withColumn("finish_time", F.col("time"))
    else:
        df = df.withColumn("finish_time", F.lit(None).cast("string"))

    if "event" in df.columns and "bout" in df.columns:
        df = df.withColumn(
            "fight_key",
            F.concat_ws("_", F.col("event"), F.col("bout"))
        )
    else:
        df = df.withColumn("fight_key", F.lit(None).cast("string"))

    df = df.select(
        "fight_id",
        "fight_key",
        "event_id",
        "fighter1_name",
        "fighter2_name",
        "winner_name",
        F.col("weight_class_clean").alias("weight_class"),
        "is_title_fight",
        "method_category",
        "method",
        F.col("round_num").alias("round"),
        "finish_time"
    )

    return df.dropDuplicates(["fight_id"])


def clean_fight_stats(df):
    print("Cleaning fight stats...")
    
    cols = [c.lower() for c in df.columns]
    
    if "fighter" in cols:
        df = df.withColumn("fighter_name", F.col("fighter"))
    else:
        df = df.withColumn("fighter_name", F.lit("Unknown"))
    
    if "event" in cols and "bout" in cols:
        df = df.withColumn(
            "fight_key",
            F.concat_ws("_", F.col("event"), F.col("bout"))
        )
    else:
        df = df.withColumn("fight_key", F.lit(None).cast("string"))
    
    sig_str_col = None
    for c in df.columns:
        if "sig" in c.lower() and "str" in c.lower():
            sig_str_col = c
            break
    
    if sig_str_col:
        col_ref = f"`{sig_str_col}`"
        sig_landed = F.regexp_extract(F.col(col_ref), r"(\d+) of", 1)
        sig_attempted = F.regexp_extract(F.col(col_ref), r"of (\d+)", 1)
        df = df.withColumn(
            "sig_strikes_landed",
            F.when(F.length(sig_landed) > 0, sig_landed.cast(IntegerType())).otherwise(F.lit(None))
        ).withColumn(
            "sig_strikes_attempted",
            F.when(F.length(sig_attempted) > 0, sig_attempted.cast(IntegerType())).otherwise(F.lit(None))
        )
    else:
        df = df.withColumn("sig_strikes_landed", F.lit(None).cast(IntegerType()))
        df = df.withColumn("sig_strikes_attempted", F.lit(None).cast(IntegerType()))
    
    if "td" in cols:
        td_landed = F.regexp_extract(F.col("td"), r"(\d+) of", 1)
        td_attempted = F.regexp_extract(F.col("td"), r"of (\d+)", 1)
        df = df.withColumn(
            "takedowns_landed",
            F.when(F.length(td_landed) > 0, td_landed.cast(IntegerType())).otherwise(F.lit(None))
        ).withColumn(
            "takedowns_attempted",
            F.when(F.length(td_attempted) > 0, td_attempted.cast(IntegerType())).otherwise(F.lit(None))
        )
    else:
        df = df.withColumn("takedowns_landed", F.lit(None).cast(IntegerType()))
        df = df.withColumn("takedowns_attempted", F.lit(None).cast(IntegerType()))
    
    if "kd" in cols:
        kd_val = F.col("kd")
        df = df.withColumn(
            "knockdowns",
            F.when((F.length(kd_val) > 0) & kd_val.isNotNull(), kd_val.cast(IntegerType())).otherwise(F.lit(None))
        )
    else:
        df = df.withColumn("knockdowns", F.lit(None).cast(IntegerType()))
    
    return df


# Event features

def build_event_features(events_df, fights_df):
    print("Building event features...")

    fight_agg = fights_df.groupBy("event_id").agg(
        F.count("*").alias("num_fights"),
        F.sum(F.col("is_title_fight").cast(IntegerType())).alias("num_title_fights"),
        F.first(F.when(F.col("is_title_fight"), F.col("weight_class"))).alias("title_weight_class")
    )

    df = events_df.join(fight_agg, on="event_id", how="left")
    
    df = df.withColumn("day_of_week", F.dayofweek("event_date"))
    df = df.withColumn("month", F.month("event_date"))
    df = df.withColumn("is_saturday", (F.col("day_of_week") == 7).cast(IntegerType()))
    
    df = df.withColumn(
        "is_las_vegas",
        F.col("city").contains("Las Vegas").cast(IntegerType())
    )
    df = df.withColumn(
        "is_usa",
        (F.col("country") == "USA").cast(IntegerType())
    )
    
    return df


def process_betting_odds(betting_df, fights_df):
    if betting_df is None:
        return None
    
    print("Processing betting odds...")
    
    betting_df = betting_df.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    betting_df = betting_df.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )
    
    betting_features = betting_df.select(
        "f1_name_norm", "f2_name_norm",
        F.col("fighter1_odds").alias("f1_closing_odds"),
        F.col("fighter2_odds").alias("f2_closing_odds"),
        F.col("fighter1_implied_prob").alias("f1_implied_prob"),
        F.col("fighter2_implied_prob").alias("f2_implied_prob"),
        F.col("odds_spread").alias("betting_spread"),
        F.when(F.col("is_competitive_matchup") == 1, True).otherwise(False).alias("is_competitive"),
        F.when(F.col("has_heavy_favorite") == 1, True).otherwise(False).alias("has_heavy_favorite")
    )
    
    print(f"  Processed {betting_features.count()} betting records")
    return betting_features


def process_sentiment(sentiment_df, events_df):
    if sentiment_df is None:
        return None
    
    print("Processing event sentiment...")
    
    sentiment_df = sentiment_df.withColumn(
        "event_name_norm",
        F.regexp_replace(F.lower(F.col("event_name")), r"[^a-z0-9]", "")
    )
    
    sentiment_features = sentiment_df.select(
        "event_name_norm",
        F.col("avg_sentiment").alias("reddit_sentiment"),
        F.col("sentiment_std").alias("sentiment_variance"),
        F.col("total_engagement").alias("reddit_engagement"),
        F.col("comment_count").alias("reddit_comments"),
        F.col("hype_score").alias("reddit_hype")
    )
    
    print(f"  Processed {sentiment_features.count()} sentiment records")
    return sentiment_features


def process_trends(trends_df, buzz_df):
    if buzz_df is None and trends_df is None:
        return None
    
    print("Processing Google Trends...")
    
    if buzz_df is not None:
        buzz_df = buzz_df.withColumn(
            "f1_name_norm",
            F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
        )
        buzz_df = buzz_df.withColumn(
            "f2_name_norm",
            F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
        )
        
        trends_features = buzz_df.select(
            "f1_name_norm", "f2_name_norm",
            F.col("combined_buzz_7d").alias("pre_event_buzz_7d"),
            F.col("combined_buzz_30d").alias("pre_event_buzz_30d"),
            F.col("max_peak_search").alias("peak_search_interest"),
            F.col("buzz_differential").alias("buzz_differential")
        )
        
        print(f"  Processed {trends_features.count()} buzz records")
        return trends_features
    
    if trends_df is not None:
        trends_agg = trends_df.groupBy("fighter_name").agg(
            F.avg("search_interest").alias("avg_search_interest"),
            F.max("search_interest").alias("max_search_interest"),
            F.stddev("search_interest").alias("search_volatility")
        )
        
        trends_agg = trends_agg.withColumn(
            "fighter_name_norm",
            F.regexp_replace(F.lower(F.col("fighter_name")), r"[^a-z0-9]", "")
        )
        
        print(f"  Processed {trends_agg.count()} trend records")
        return trends_agg
    
    return None


def enrich_fights_with_external(fights_df, betting_df, trends_df, graph_df):
    print("Enriching fights with external data...")
    
    df = fights_df
    
    df = df.withColumn(
        "f1_name_norm",
        F.regexp_replace(F.lower(F.col("fighter1_name")), r"[^a-z0-9]", "")
    )
    df = df.withColumn(
        "f2_name_norm",
        F.regexp_replace(F.lower(F.col("fighter2_name")), r"[^a-z0-9]", "")
    )
    
    if betting_df is not None:
        df = df.join(
            betting_df,
            on=["f1_name_norm", "f2_name_norm"],
            how="left"
        )
        print("  Added betting features")

    if trends_df is not None and "f1_name_norm" in trends_df.columns:
        df = df.join(
            trends_df,
            on=["f1_name_norm", "f2_name_norm"],
            how="left"
        )
        print("  Added trends features")
    
    if graph_df is not None:
        graph_df = graph_df.withColumn(
            "name_norm",
            F.regexp_replace(F.lower(F.col("name")), r"[^a-z0-9]", "")
        )
        
        graph_cols = [c.lower() for c in graph_df.columns]
        pagerank_col = "pagerank_score" if "pagerank_score" in graph_cols else "pagerank"
        network_col = "num_opponents" if "num_opponents" in graph_cols else (
            "in_degree" if "in_degree" in graph_cols else None
        )
        community_col = "community_id" if "community_id" in graph_cols else "community"
        
        f1_selections = [F.col("name_norm").alias("f1_name_norm")]
        f2_selections = [F.col("name_norm").alias("f2_name_norm")]
        
        if pagerank_col in graph_cols:
            f1_selections.append(F.col(pagerank_col).alias("f1_pagerank"))
            f2_selections.append(F.col(pagerank_col).alias("f2_pagerank"))
        
        if network_col and network_col in graph_cols:
            f1_selections.append(F.col(network_col).alias("f1_network_size"))
            f2_selections.append(F.col(network_col).alias("f2_network_size"))
        
        if community_col in graph_cols:
            f1_selections.append(F.col(community_col).alias("f1_community"))
            f2_selections.append(F.col(community_col).alias("f2_community"))
        
        graph_f1 = graph_df.select(*f1_selections)
        df = df.join(graph_f1, on="f1_name_norm", how="left")

        graph_f2 = graph_df.select(*f2_selections)
        df = df.join(graph_f2, on="f2_name_norm", how="left")
        
        df_cols = [c.lower() for c in df.columns]
        
        # Calculate combined graph features
        f1_pr = F.coalesce(F.col("f1_pagerank"), F.lit(0.0))
        f2_pr = F.coalesce(F.col("f2_pagerank"), F.lit(0.0))
        if "f1_pagerank" in df_cols and "f2_pagerank" in df_cols:
            df = df.withColumn("combined_pagerank", f1_pr + f2_pr)
        else:
            df = df.withColumn("combined_pagerank", F.lit(0.5))
        
        f1_net = F.coalesce(F.col("f1_network_size"), F.lit(0))
        f2_net = F.coalesce(F.col("f2_network_size"), F.lit(0))
        if "f1_network_size" in df_cols and "f2_network_size" in df_cols:
            df = df.withColumn("combined_network_size", f1_net + f2_net)
        else:
            df = df.withColumn("combined_network_size", F.lit(10))
        
        if "f1_community" in df_cols and "f2_community" in df_cols:
            df = df.withColumn("same_community", (F.col("f1_community") == F.col("f2_community")).cast(IntegerType()))
        else:
            df = df.withColumn("same_community", F.lit(0))
        
        print("  Added graph features")
    
    df = df.fillna({
        "betting_spread": 0.1,
        "is_competitive": True,
        "has_heavy_favorite": False,
        "pre_event_buzz_7d": 50.0,
        "combined_pagerank": 0.5,
        "combined_network_size": 10
    })
    
    print(f"  Enriched {df.count()} fights")
    return df


def enrich_events_with_sentiment(events_df, sentiment_df):
    if sentiment_df is None:
        return events_df

    print("Enriching events with sentiment...")

    events_df = events_df.withColumn(
        "event_name_norm",
        F.regexp_replace(F.lower(F.col("event_name")), r"[^a-z0-9]", "")
    )

    df = events_df.join(sentiment_df, on="event_name_norm", how="left")
    
    df = df.fillna({
        "reddit_sentiment": 0.0,
        "reddit_engagement": 100,
        "reddit_hype": 0.5
    })
    
    print(f"  Enriched {df.count()} events")
    return df


def save_parquet(df, output_path, partition_cols=None):
    print(f"Saving to {output_path}...")
    
    writer = df.write.mode("overwrite")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.parquet(output_path)
    print(f"  Saved {df.count()} rows")


def run_etl(data_dir, output_dir, local=True):
    spark = make_spark("UFC-ETL", local=local)
    
    try:
        raw_dfs = load_data(spark, data_dir)
        
        print(f"Loaded datasets: {list(raw_dfs.keys())}")
        
        external_dfs = load_external_data(spark, data_dir)
        print(f"Loaded external datasets: {list(external_dfs.keys())}")
        
        graph_features = load_graph_features(spark, data_dir)
        
        if "events" in raw_dfs:
            events_clean = clean_events(raw_dfs["events"])
            events_clean = events_clean.cache()
            print(f"  Events cleaned and cached")
        else:
            print("ERROR: No events data!")
            return
            
        fighters_clean = None
        if "fighter_tott" in raw_dfs:
            fighters_clean = clean_fighters(raw_dfs["fighter_tott"])
            fighters_clean = fighters_clean.cache()
            print(f"  Fighters cleaned and cached")
        elif "fighter_details" in raw_dfs:
            fighters_clean = clean_fighters(raw_dfs["fighter_details"])
            fighters_clean = fighters_clean.cache()
            print(f"  Fighters cleaned (limited data) and cached")
        elif "fighters" in raw_dfs:
            fighters_clean = clean_fighters(raw_dfs["fighters"])
            fighters_clean = fighters_clean.cache()
            print(f"  Fighters cleaned and cached")
        else:
            print("  Warning: No fighter data found")
            
        fights_clean = None
        if "fight_details" in raw_dfs:
            fights_clean = clean_fights(raw_dfs["fight_details"])
            fights_clean = fights_clean.cache()
            print(f"  Fights cleaned and cached")
        elif "fight_results" in raw_dfs:
            fights_clean = clean_fights(raw_dfs["fight_results"])
            fights_clean = fights_clean.cache()
            print(f"  Fights cleaned and cached")
        else:
            print("  Warning: No fight data found")
            
        fight_stats_clean = None
        if "fight_stats" in raw_dfs:
            fight_stats_clean = clean_fight_stats(raw_dfs["fight_stats"])
            print(f"  Fight stats cleaned: {fight_stats_clean.count()} rows")

            if fights_clean is not None:
                fight_id_map = fights_clean.select("fight_id", "fight_key").filter(F.col("fight_key").isNotNull())
                fight_stats_clean = fight_stats_clean.join(
                    fight_id_map,
                    on="fight_key",
                    how="left"
                )
                print(f"  Added fight_id to {fight_stats_clean.filter(F.col('fight_id').isNotNull()).count()} fight stats rows")
        else:
            print("  Warning: No fight stats data found")
        
        betting_features = None
        if "betting_odds" in external_dfs:
            betting_features = process_betting_odds(external_dfs["betting_odds"], fights_clean)
        
        sentiment_features = None
        if "event_sentiment" in external_dfs:
            sentiment_features = process_sentiment(external_dfs["event_sentiment"], events_clean)
        
        trends_features = None
        if "fighter_buzz" in external_dfs:
            trends_features = process_trends(
                external_dfs.get("google_trends"),
                external_dfs["fighter_buzz"]
            )
        elif "google_trends" in external_dfs:
            trends_features = process_trends(external_dfs["google_trends"], None)
        
        fights_enriched = fights_clean
        if fights_clean is not None:
            fights_enriched = enrich_fights_with_external(
                fights_clean,
                betting_features,
                trends_features,
                graph_features
            )

        if fights_enriched is not None:
            fights_enriched = fights_enriched.cache()
            print("  Cached enriched fights for reuse")
        
        events_enriched = events_clean
        if sentiment_features is not None:
            events_enriched = enrich_events_with_sentiment(events_clean, sentiment_features)
        
        if fights_enriched:
            event_features = build_event_features(events_enriched, fights_enriched)
        else:
            event_features = events_enriched
        
        processed_dir = os.path.join(output_dir, "processed")
        os.makedirs(processed_dir, exist_ok=True)
        
        save_parquet(
            events_enriched,
            os.path.join(processed_dir, "events"),
            partition_cols=["year"]
        )
        
        if fighters_clean:
            save_parquet(
                fighters_clean,
                os.path.join(processed_dir, "fighters")
            )
        
        if fights_enriched:
            save_parquet(
                fights_enriched,
                os.path.join(processed_dir, "fights")
            )
        
        if fight_stats_clean:
            save_parquet(
                fight_stats_clean,
                os.path.join(processed_dir, "fight_stats")
            )
        
        save_parquet(
            event_features,
            os.path.join(processed_dir, "event_features"),
            partition_cols=["year"]
        )
        
        print("\nSummary:")
        print(f"  Betting: {'Yes' if betting_features is not None else 'No'}")
        print(f"  Trends: {'Yes' if trends_features is not None else 'No'}")
        print(f"  Sentiment: {'Yes' if sentiment_features is not None else 'No'}")
        print(f"  Graph: {'Yes' if graph_features is not None else 'No'}")
        print("\nDone!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UFC ETL Pipeline")
    parser.add_argument("--data-dir", default="./data", help="Input data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    parser.add_argument("--cluster", action="store_true", help="Run in cluster mode")
    
    args = parser.parse_args()
    
    run_etl(args.data_dir, args.output_dir, local=not args.cluster)
