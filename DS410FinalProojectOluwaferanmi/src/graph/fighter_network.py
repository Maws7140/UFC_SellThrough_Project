from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType
import argparse
import os

try:
    from graphframes import GraphFrame
    GRAPHFRAMES_AVAILABLE = True
except ImportError:
    GRAPHFRAMES_AVAILABLE = False
    print("Warning: GraphFrames not installed. Install it ")


def create_spark_session():
    builder = SparkSession.builder
    builder = builder.appName("UFC-Fighter-Network")
    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def load_fight_data(spark, data_dir):
    print("Loading fight data for graph construction...")
    
    processed_fights_path = os.path.join(data_dir, "processed", "fights")
    processed_events_path = os.path.join(data_dir, "processed", "events")
    
    fights = None
    events = None
    
    if os.path.exists(processed_fights_path):
        print("  Loading from processed parquet files...")
        fights = spark.read.parquet(processed_fights_path)
        num_fights = fights.count()
        print(f"  Loaded {num_fights} fights from parquet")
    else:
        raw_fights_path = os.path.join(data_dir, "raw", "fight_results.csv")
        if os.path.exists(raw_fights_path):
            print("  Loading from raw CSV file...")
            fights = spark.read.csv(raw_fights_path, header=True, inferSchema=True)
            
            for column_name in fights.columns:
                fights = fights.withColumnRenamed(column_name, column_name.lower())
            
            num_fights = fights.count()
            print(f"  Loaded {num_fights} fights from CSV")
        else:
            print("ERROR: No fight data found!")
            print(f"  Looked for: {processed_fights_path}")
            print(f"  And also: {raw_fights_path}")
            return None, None
    
    if os.path.exists(processed_events_path):
        events = spark.read.parquet(processed_events_path)
    
    return fights, events


def build_fighter_graph(fights_df, events_df=None):
    # Build graph: fighters = nodes, fights = edges
    print("Building fighter graph...")
    
    if not GRAPHFRAMES_AVAILABLE:
        print("ERROR: GraphFrames not available. Cannot build graph.")
        return None
    
    # Find fighter name columns
    fighter1_column_name = None
    fighter2_column_name = None
    
    if "fighter1_name" in fights_df.columns:
        fighter1_column_name = "fighter1_name"
    elif "fighter1" in fights_df.columns:
        fighter1_column_name = "fighter1"
    
    if "fighter2_name" in fights_df.columns:
        fighter2_column_name = "fighter2_name"
    elif "fighter2" in fights_df.columns:
        fighter2_column_name = "fighter2"
    
    if fighter1_column_name is None and "bout" in fights_df.columns:
        print("  Extracting fighter names from 'bout' column...")
        fights_df = fights_df.withColumn(
            "fighter1_name",
            F.trim(F.regexp_extract(F.col("bout"), r"^(.+?)\s+vs\.?\s+", 1))
        )
        fights_df = fights_df.withColumn(
            "fighter2_name",
            F.trim(F.regexp_extract(F.col("bout"), r"\s+vs\.?\s+(.+)$", 1))
        )
        fighter1_column_name = "fighter1_name"
        fighter2_column_name = "fighter2_name"
    
    if fighter1_column_name is None or fighter2_column_name is None:
        print(f"ERROR: Could not find fighter columns in: {fights_df.columns}")
        return None
    
    print("  Collecting all unique fighters...")
    fighters_from_column1 = fights_df.select(F.col(fighter1_column_name).alias("name"))
    fighters_from_column2 = fights_df.select(F.col(fighter2_column_name).alias("name"))
    all_fighters_unique = fighters_from_column1.union(fighters_from_column2).distinct()
    
    # GraphFrames needs numeric IDs
    fighters_with_ids = all_fighters_unique.withColumn("id", F.monotonically_increasing_id())
    nodes = fighters_with_ids.select("id", "name")
    name_to_id_lookup = nodes.select("name", "id")
    
    print("  Creating edges between fighters...")
    fight_pairs = fights_df.select(
        F.col(fighter1_column_name).alias("src_name"),
        F.col(fighter2_column_name).alias("dst_name")
    )
    
    edges_with_src_id = fight_pairs.join(
        name_to_id_lookup.withColumnRenamed("id", "src").withColumnRenamed("name", "src_name"),
        on="src_name"
    )
    edges_with_both_ids = edges_with_src_id.join(
        name_to_id_lookup.withColumnRenamed("id", "dst").withColumnRenamed("name", "dst_name"),
        on="dst_name"
    )
    edges = edges_with_both_ids.select("src", "dst")
    # Count multiple fights between same pair
    edges = edges.groupBy("src", "dst").agg(F.count("*").alias("fight_count"))
    
    num_nodes = nodes.count()
    num_edges = edges.count()
    print(f"  Created graph with {num_nodes} fighters (nodes)")
    print(f"  Created graph with {num_edges} unique matchups (edges)")
    
    graph = GraphFrame(nodes, edges)
    return graph


def compute_pagerank(graph, reset_prob=0.15, max_iter=20):
    # PageRank measures importance so fighters who fight important fighters are more important
    print("Computing PageRank...")
    
    if graph is None:
        return None
    
    pagerank_results = graph.pageRank(resetProbability=reset_prob, maxIter=max_iter)
    pagerank_df = pagerank_results.vertices.select(
        "id", 
        "name", 
        F.col("pagerank").alias("pagerank_score")
    )
    
    print("  Top 10 fighters by PageRank:")
    pagerank_df.orderBy(F.desc("pagerank_score")).show(10, truncate=False)
    
    return pagerank_df


def compute_connected_components(graph):
    # Groups of fighters connected through fights
    print("Computing Connected Components...")
    
    if graph is None:
        return None
    
    # Need checkpoint dir for this algorithm
    import tempfile
    checkpoint_directory = os.path.join(tempfile.gettempdir(), "spark_checkpoints")
    os.makedirs(checkpoint_directory, exist_ok=True)
    graph.vertices.sparkSession.sparkContext.setCheckpointDir(checkpoint_directory)
    
    components_result = graph.connectedComponents()
    component_size_counts = components_result.groupBy("component").count().orderBy(F.desc("count"))
    
    num_components = component_size_counts.count()
    print(f"  Found {num_components} connected components")
    print("  Largest components:")
    component_size_counts.show(5)
    
    result_df = components_result.select(
        "id", 
        "name", 
        F.col("component").alias("component_id")
    )
    
    return result_df


def compute_triangle_count(graph):
    # Count triangles when 3 fighters all fought each other measures clustering
    print("Computing Triangle Count...")
    
    if graph is None:
        return None
    
    triangles_result = graph.triangleCount()
    triangle_df = triangles_result.select(
        "id", 
        "name",
        F.col("count").alias("triangle_count")
    )
    
    print("  Top 10 fighters by triangle count:")
    triangle_df.orderBy(F.desc("triangle_count")).show(10, truncate=False)
    
    return triangle_df


def compute_degree_centrality(graph):
    # Just count how many unique opponents each fighter has
    print("Computing Degree Centrality...")
    
    if graph is None:
        return None
    
    degrees_result = graph.degrees
    degrees_with_names = degrees_result.join(graph.vertices, on="id")
    degree_df = degrees_with_names.select(
        "id", 
        "name", 
        F.col("degree").alias("num_opponents")
    )
    
    print("  Top 10 fighters by number of unique opponents:")
    degree_df.orderBy(F.desc("num_opponents")).show(10, truncate=False)
    
    return degree_df


def compute_label_propagation(graph, max_iter=5):
    # Find communities  groups that fight each other a lot
    print("Running Label Propagation (Community Detection)...")
    
    if graph is None:
        return None
    
    communities_result = graph.labelPropagation(maxIter=max_iter)
    community_df = communities_result.select(
        "id", 
        "name",
        F.col("label").alias("community_id")
    )
    
    community_size_counts = community_df.groupBy("community_id").count().orderBy(F.desc("count"))
    num_communities = community_size_counts.count()
    print(f"  Found {num_communities} communities")
    print("  Largest communities:")
    community_size_counts.show(5)
    
    return community_df


def combine_graph_features(pagerank_df, components_df, triangles_df, degrees_df, communities_df):
    # Join all the metrics into one dataframe
    print("Combining graph features...")
    
    if pagerank_df is None:
        return None
    
    combined_df = pagerank_df
    
    if components_df is not None:
        combined_df = combined_df.join(components_df.select("id", "component_id"), on="id", how="left")
    if triangles_df is not None:
        combined_df = combined_df.join(triangles_df.select("id", "triangle_count"), on="id", how="left")
    if degrees_df is not None:
        combined_df = combined_df.join(degrees_df.select("id", "num_opponents"), on="id", how="left")
    if communities_df is not None:
        combined_df = combined_df.join(communities_df.select("id", "community_id"), on="id", how="left")
    
    # Fill missing values with 0
    combined_df = combined_df.fillna(0)
    print(f"  Combined features for {combined_df.count()} fighters")
    
    return combined_df


def save_graph_features(df, output_dir):
    if df is None:
        print("No graph features to save!")
        return
    
    output_path = os.path.join(output_dir, "features", "graph_features")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Saving graph features to {output_path}...")
    df.write.mode("overwrite").parquet(output_path)
    print("  Done!")


def run_graph_analysis(data_dir, output_dir):
    print("=" * 60)
    print("UFC Fighter Network Analysis")
    print("=" * 60)
    
    if not GRAPHFRAMES_AVAILABLE:
        print("\nERROR: GraphFrames is not installed!")
        print("Install with: pip install graphframes")
        print("\nCreating placeholder graph features instead...")
        
        spark = create_spark_session()
        fights, events = load_fight_data(spark, data_dir)
        
        if fights is not None:
            create_placeholder_features(spark, fights, output_dir)
        
        spark.stop()
        return
    
    spark = create_spark_session()
    
    try:
        fights, events = load_fight_data(spark, data_dir)
        if fights is None:
            print("No fight data available!")
            return
        
        graph = build_fighter_graph(fights, events)
        if graph is None:
            print("Could not build graph!")
            return
        
        print("\nComputing graph metrics...")
        pagerank_df = compute_pagerank(graph)
        components_df = compute_connected_components(graph)
        triangles_df = compute_triangle_count(graph)
        degrees_df = compute_degree_centrality(graph)
        communities_df = compute_label_propagation(graph)
        
        graph_features = combine_graph_features(
            pagerank_df, components_df, triangles_df, 
            degrees_df, communities_df
        )
        save_graph_features(graph_features, output_dir)
        
        print("\n" + "=" * 60)
        print("Graph Analysis Complete!")
        print("=" * 60)
        
    finally:
        spark.stop()


def create_placeholder_features(spark, fights_df, output_dir):
    #  just count fights if it doesnt work lol
    print("Creating placeholder graph features (GraphFrames not available)...")
    
    fighter1_column_name = None
    fighter2_column_name = None
    
    if "fighter1_name" in fights_df.columns:
        fighter1_column_name = "fighter1_name"
    elif "fighter1" in fights_df.columns:
        fighter1_column_name = "fighter1"
    
    if "fighter2_name" in fights_df.columns:
        fighter2_column_name = "fighter2_name"
    elif "fighter2" in fights_df.columns:
        fighter2_column_name = "fighter2"
    
    if fighter1_column_name is None and "bout" in fights_df.columns:
        fights_df = fights_df.withColumn(
            "fighter1_name",
            F.trim(F.regexp_extract(F.col("bout"), r"^(.+?)\s+vs\.?\s+", 1))
        )
        fights_df = fights_df.withColumn(
            "fighter2_name",
            F.trim(F.regexp_extract(F.col("bout"), r"\s+vs\.?\s+(.+)$", 1))
        )
        fighter1_column_name = "fighter1_name"
        fighter2_column_name = "fighter2_name"
    
    fighter1_counts = fights_df.groupBy(F.col(fighter1_column_name).alias("name")).agg(F.count("*").alias("fights_as_f1"))
    fighter2_counts = fights_df.groupBy(F.col(fighter2_column_name).alias("name")).agg(F.count("*").alias("fights_as_f2"))
    
    all_fighters = fighter1_counts.join(fighter2_counts, on="name", how="outer").fillna(0)
    all_fighters = all_fighters.withColumn("num_opponents", F.col("fights_as_f1") + F.col("fights_as_f2"))
    
    # Simple normalized score
    max_fights = all_fighters.agg(F.max("num_opponents")).collect()[0][0]
    all_fighters = all_fighters.withColumn("pagerank_score", F.col("num_opponents") / F.lit(max_fights))
    all_fighters = all_fighters.withColumn("id", F.monotonically_increasing_id())
    # need GraphFrames, so set to 0
    all_fighters = all_fighters.withColumn("component_id", F.lit(0))
    all_fighters = all_fighters.withColumn("triangle_count", F.lit(0))
    all_fighters = all_fighters.withColumn("community_id", F.lit(0))
    
    result = all_fighters.select(
        "id", "name", "pagerank_score", "component_id",
        "triangle_count", "num_opponents", "community_id"
    )
    
    print(f"  Created placeholder features for {result.count()} fighters")
    save_graph_features(result, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UFC Fighter Network Analysis")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    
    args = parser.parse_args()
    
    run_graph_analysis(args.data_dir, args.output_dir)

