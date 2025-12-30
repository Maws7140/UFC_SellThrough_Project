# UFC Sell-Through Prediction Project

**DS/CMPSC 410 - Programming Models for Big Data**  
**Fall 2025 - Penn State**  
**Student: Oluwaferanmi**

## What This Project Does

I built a system that predicts how many tickets UFC events will sell. The main metric is "sell-through" which is just:

```
sell_through = tickets_sold / venue_capacity
```

For example, if a venue holds 20,000 people and 18,000 tickets sell, that's a 90% sell-through.

## Why I Chose This Project

I wanted to work with something I'm interested while learning how to:
- Process large datasets with PySpark (we have ~40,000 fight statistics!)
- Use window functions for rolling calculations
- Train ML models with Spark MLlib
- Run jobs on the Roar cluster
- Work with graph data using GraphFrames
- Integrate multiple external data sources

## The Data

### Primary Data Sources

**1. UFCStats.com** (via a GitHub repo that scrapes it daily)
- 756 events going back to UFC 1 in 1993
- 8,400+ fights with results
- 40,000 round-by-round statistics (strikes, takedowns, etc.)
- 4,400+ fighter profiles

**2. Wikipedia** (I wrote a scraper for this)
- Attendance numbers for ~177 events
- Gate revenue (ticket sales in $)
- Venue capacities

### External Data Sources (Expanded)

**3. Betting Odds**
- Historical betting lines for ~8,400 fights
- Opening/closing odds, line movement
- Implied probabilities and spread

**4. Google Trends**
- Search interest for 4,400+ fighters
- Pre-event buzz metrics (7-day, 30-day)
- ~2.3M trend data points

**5. Reddit Sentiment** (r/MMA)
- ~380,000 comments from event discussion threads
- Sentiment scores (positive/negative/neutral)
- Event hype scores and engagement metrics

### Data Volume

| Source | Records | Size |
|--------|---------|------|
| UFC Stats (raw) | ~53,000 | ~11 MB |
| Betting Odds | ~84,000 | ~50 MB |
| Google Trends | ~2.3M | ~200 MB |
| Reddit Comments | ~380,000 | ~150 MB |
| Graph Features | ~4,400 | ~20 MB |
| **Total** | **~2.8M** | **~430 MB** |

## Project Structure

```
UFC_SellThrough_Project/
|-- data/
|   |-- raw/                  <- Original CSV files
|   |-- processed/            <- Cleaned Parquet files
|   |-- features/             <- Feature tables + graph features
|   |-- external/             <- Betting, trends, sentiment data
|   |-- models/               <- Trained models + metrics
|
|-- src/
|   |-- etl/
|   |   |-- spark_etl.py              <- Main ETL (with external data)
|   |   |-- ingest.py                 <- Data downloader
|   |   |-- scrape_betting_odds.py    <- Betting odds collector
|   |   |-- fetch_google_trends.py    <- Google Trends fetcher
|   |   |-- scrape_reddit_sentiment.py <- Reddit sentiment analyzer
|   |   |-- scrape_all_attendance.py  <- Wikipedia scraper
|   |
|   |-- graph/
|   |   |-- fighter_network.py        <- GraphFrames analysis
|   |
|   |-- features/
|   |   |-- feature_engineering.py    <- Rolling stats + external features
|   |
|   |-- models/
|   |   |-- train.py                  <- GBT model training
|   |
|   |-- optimizer/
|       |-- card_optimizer.py         <- Fight card optimization
|
|-- scripts/
|   |-- run_pipeline.slurm            <- Full pipeline for Roar
|
|-- requirements.txt
|-- README.md (this file)
```

## How to Run It

### On Your Local Machine (for testing)

```bash
# Install dependencies
pip install pyspark pandas numpy beautifulsoup4 requests lxml pytrends textblob graphframes

# Step 1: Download base data
python src/etl/ingest.py --data-dir ./data

# Step 2: Collect external data
python src/etl/scrape_betting_odds.py --data-dir ./data --output-dir ./data
python src/etl/fetch_google_trends.py --data-dir ./data --output-dir ./data
python src/etl/scrape_reddit_sentiment.py --data-dir ./data --output-dir ./data

# Step 3: Run ETL (with external data integration)
spark-submit src/etl/spark_etl.py --data-dir ./data --output-dir ./data

# Step 4: Run graph analysis
spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 \
    src/graph/fighter_network.py --data-dir ./data --output-dir ./data

# Step 5: Run feature engineering
spark-submit src/features/feature_engineering.py --data-dir ./data --output-dir ./data

# Step 6: Train the model
spark-submit src/models/train.py --data-dir ./data --test-year 2024

# Step 7: Run the optimizer demo
python src/optimizer/card_optimizer.py
```

### On ICDS Roar

```bash
# SSH in
ssh YOUR_USERNAME@submit.hpc.psu.edu

# Upload project folder

# Set up environment
module load anaconda3
module load spark/3.4.1
conda create -n ds410 python=3.10 pyspark pandas numpy beautifulsoup4 requests pytrends textblob -y
conda activate ds410

# Run the whole pipeline (now includes external data + graph analysis)
sbatch scripts/run_pipeline.slurm

# Check status
squeue -u $USER
```

## Key Concepts I Used (from class)

### 1. PySpark DataFrames
Instead of using pandas (which loads everything into memory), I used PySpark DataFrames which can handle data larger than RAM by distributing it across workers.

### 2. Window Functions
Window functions let you calculate things like "win rate in last 5 fights" without writing loops. Example:

```python
# Calculate rolling win rate for each fighter
window = Window.partitionBy("fighter_id").orderBy("event_date").rowsBetween(-5, -1)
df = df.withColumn("win_rate_last5", F.avg("is_win").over(window))
```

### 3. GraphFrames 
Used GraphFrames to build a fighter network graph and compute:
- **PageRank**: Identifies important fighters based on opponent quality
- **Connected Components**: Finds fighter clusters
- **Triangle Count**: Detects tight-knit divisions
- **Label Propagation**: Community detection

```python
# Build fighter graph and compute PageRank
graph = GraphFrame(fighters, fights)
pagerank = graph.pageRank(resetProbability=0.15, maxIter=20)
```

### 4. Parquet Format
I saved processed data as Parquet instead of CSV because:
- It's columnar (fast for analytics queries)
- It compresses well
- It preserves data types

### 5. Spark MLlib
Used GBTRegressor (Gradient Boosted Trees) because:
- Works well for regression problems
- Handles non-linear relationships
- Built into Spark so it scales

### 6. Time-Based Splits
Super important for avoiding data leakage! I trained on 2016-2023 data and tested on 2024. You can't use future fights to predict past attendance.

## Features I Created

### Base Features (Window Functions)

| Feature | Description | How I Calculated It |
|---------|-------------|---------------------|
| win_rate_last5 | Fighter's win rate in last 5 fights | Window function with avg() |
| finish_rate_last5 | How often they finish fights (KO/Sub) | Window function |
| days_since_last_fight | Layoff period | datediff() between fights |
| num_title_fights | Number of title fights on card | count() with filter |
| is_ppv | Is it a Pay-Per-View event? | String matching on event name |
| is_las_vegas | Is it in Vegas? | Location contains "Vegas" |

### Graph Features (NEW)

| Feature | Description |
|---------|-------------|
| combined_pagerank | Sum of both fighters' PageRank scores |
| pagerank_differential | Difference in PageRank (mismatch indicator) |
| combined_network_size | Total unique opponents fought |
| same_community | Are fighters in same weight class cluster? |

### Betting Features (NEW)

| Feature | Description |
|---------|-------------|
| betting_spread | How lopsided the odds are |
| is_competitive | Spread < 10% (close matchup) |
| has_heavy_favorite | One fighter > 70% implied probability |

### Trends/Sentiment Features (NEW)

| Feature | Description |
|---------|-------------|
| pre_event_buzz_7d | Combined search interest 7 days before |
| reddit_sentiment | Average sentiment from r/MMA threads |
| reddit_hype | Engagement-weighted hype score |

## Results

### Model Performance (Best Run - 36 Features)

- **Test RMSE**: 0.3231
- **Test MAE**: 0.1801
- **Test R²**: 0.4763 (explains 47.6% of variance)
- **Features used**: 36
- **Training events**: 565 (events before 2024)
- **Test events**: 80 (events in 2024 and later)

### Model Performance (Full Pipeline - 9 Features)

- **Test RMSE**: 0.3829
- **Test MAE**: 0.3460
- **Test R²**: 0.2644 (explains 26.4% of variance)
- **Features used**: 9
- **Training events**: 565
- **Test events**: 80

**Key Insight**: The extended feature set (36 features) shows an 80% improvement in R² compared to the limited feature set, demonstrating the value of comprehensive feature engineering including graph analytics and external data sources.

## Future Improvements
- Add real-time betting odds API integration
- Implement streaming pipeline for live event predictions
- Train ensemble models (combine GBT with neural networks)
- Build a web dashboard for predictions
- Add PPV buy rate predictions

## References

- UFCStats.com - Main data source
- [Greco1899/scrape_ufc_stats](https://github.com/Greco1899/scrape_ufc_stats) - Pre-scraped UFC data
- Spark MLlib docs - https://spark.apache.org/docs/latest/ml-guide.html
- GraphFrames docs - https://graphframes.github.io/graphframes/docs/_site/
- DS 410 course materials
