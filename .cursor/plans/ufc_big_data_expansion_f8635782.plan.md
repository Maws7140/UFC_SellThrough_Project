---
name: UFC Big Data Expansion
overview: Expand the UFC Sell-Through project by adding external data sources (betting odds, social media, Google Trends) and implementing GraphX fighter network analysis to transform ~11MB into ~500MB+ of data with meaningful big data processing.
todos:
  - id: graph-setup
    content: Create src/graph/fighter_network.py with GraphFrames PageRank and community detection
    status: completed
  - id: betting-scraper
    content: Create src/etl/scrape_betting_odds.py to fetch historical UFC betting lines
    status: completed
  - id: trends-fetcher
    content: Create src/etl/fetch_google_trends.py using pytrends for fighter search volume
    status: completed
  - id: reddit-scraper
    content: Create src/etl/scrape_reddit_sentiment.py for r/MMA event discussion threads
    status: completed
  - id: update-etl
    content: Update spark_etl.py to process and join all new data sources
    status: completed
  - id: update-features
    content: Update feature_engineering.py with graph metrics, betting, and sentiment features
    status: completed
  - id: update-pipeline
    content: Update run_pipeline.slurm to include graph analysis step
    status: completed
  - id: update-readme
    content: Update README.md with new architecture and data sources
    status: completed
---

# UFC Big Data Expansion Plan

## Current State

- ~11 MB of data (756 events, 40K fight stats)
- Basic ETL + Feature Engineering + GBT Model
- Only 177 events with attendance labels

## Target State

- ~500MB+ of data across multiple sources
- Graph-based fighter network analysis with GraphFrames
- External data integration (betting, trends, social)
- Richer feature set for better predictions

---

## Phase 1: External Data Integration

### 1.1 Historical Betting Odds Data

Add fighter betting odds to predict event popularity (favorites vs underdogs draw differently).

**Data Source**: BestFightOdds.com historical data or Kaggle UFC datasets

- ~8,400 fights x 10+ odds snapshots = ~84,000+ rows
- Features: opening odds, closing odds, line movement, implied probability

**New file**: `src/etl/scrape_betting_odds.py`

- Scrape historical odds per fight
- Calculate betting features: favorite/underdog spread, public betting %

### 1.2 Google Trends Fighter Popularity

Use pytrends to get search interest for fighters before events.

**Data Volume**: 4,400 fighters x 52 weeks x 10 years = ~2.3M data points

**New file**: `src/etl/fetch_google_trends.py`

- Query Google Trends API for each fighter
- Calculate pre-event buzz (7-day, 30-day search volume before fight)

### 1.3 Reddit/Social Media Sentiment

Scrape r/MMA subreddit for fight discussion threads.

**Data Volume**: ~756 events x 500+ comments each = ~380,000+ comments

**New file**: `src/etl/scrape_reddit_sentiment.py`

- Use PRAW (Reddit API) to get pre-event discussion threads
- Run basic sentiment analysis (positive/negative/neutral)
- Aggregate to event-level hype scores

---

## Phase 2: GraphFrames Fighter Network Analysis

### 2.1 Build Fighter Graph

Create a graph where:

- **Nodes** = Fighters (4,400+)
- **Edges** = Fights between them (8,400+)
- **Edge weights** = Recency, result, method

**New file**: `src/graph/fighter_network.py`

Key analyses:

1. **PageRank**: Which fighters are most "connected" through quality opponents?
2. **Connected Components**: Find fighter clusters (weight class communities)
3. **Shortest Path**: Degrees of separation between any two fighters
4. **Triangle Count**: Training camp/gym clustering detection

### 2.2 Graph-Based Features

Extract graph metrics as model features:

- `pagerank_score`: Fighter's network importance
- `opponent_avg_pagerank`: Quality of competition faced
- `component_id`: Which fighter cluster they belong to
- `betweenness_centrality`: Bridge fighters between weight classes

These features capture "star power" better than simple win/loss records.

---

## Phase 3: Updated Pipeline Architecture

### 3.1 New Data Flow

```
[Betting Odds] ----\
[Google Trends] -----> [Spark ETL] --> [GraphFrames] --> [Feature Engineering] --> [Model]
[Reddit Data] -----/       |                  |
[UFC Stats] ------/        v                  v
                     [processed/]      [graph_features/]
```

### 3.2 Updated Files

| File | Changes |

|------|---------|

| [`src/etl/ingest.py`](src/etl/ingest.py) | Add betting, trends, reddit download functions |

| [`src/etl/spark_etl.py`](src/etl/spark_etl.py) | Process new data sources, join with fights |

| [`src/graph/fighter_network.py`](src/graph/fighter_network.py) | NEW - GraphFrames analysis |

| [`src/features/feature_engineering.py`](src/features/feature_engineering.py) | Add graph features, betting features, sentiment features |

| [`src/models/train.py`](src/models/train.py) | Update feature list with new columns |

| [`scripts/run_pipeline.slurm`](scripts/run_pipeline.slurm) | Add graph analysis step |

---

## Phase 4: Expected Data Volume Increase

| Data Source | Current | After Expansion |

|-------------|---------|-----------------|

| UFC Stats | 11 MB | 11 MB |

| Betting Odds | 0 | ~50 MB |

| Google Trends | 0 | ~200 MB |

| Reddit Comments | 0 | ~150 MB |

| Graph Features | 0 | ~20 MB |

| **Total** | **11 MB** | **~430 MB** |

With processing, intermediate data, and Parquet files: **~500MB-1GB total pipeline**

---

## Implementation Order

1. Set up GraphFrames with existing fight data (demonstrates graph processing)
2. Add betting odds scraper and integration
3. Add Google Trends data fetcher
4. Add Reddit sentiment scraper
5. Update feature engineering to use all new sources
6. Retrain model with expanded features
7. Update README with new architecture diagram