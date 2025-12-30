import os
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import pytrends
try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    PYTRENDS_AVAILABLE = False
    logger.warning("pytrends not installed. Install with: pip install pytrends")


def create_pytrends_client():
    # Create pytrends client
    if not PYTRENDS_AVAILABLE:
        return None
    
    try:
        pytrends = TrendReq(hl='en-US', tz=300, retries=3, backoff_factor=0.5)
        return pytrends
    except Exception as e:
        logger.error(f"Failed to create pytrends client: {e}")
        return None


def fetch_fighter_trends(fighter_name, start_date, end_date, pytrends):
    # Fetch Google Trends data for a single fighter
    if pytrends is None:
        return pd.DataFrame()
    
    try:
        # Adding "UFC" to the query to get more relevant results
        keyword = f"{fighter_name} UFC"
        
        pytrends.build_payload(
            kw_list=[keyword],
            timeframe=f"{start_date} {end_date}",
            geo='',
            gprop=''
        )
        
        df = pytrends.interest_over_time()
        
        if df.empty:
            return pd.DataFrame()
        
        # Clean up the DataFrame
        df = df.reset_index()
        df = df.rename(columns={keyword: "search_interest"})
        df["fighter_name"] = fighter_name
        
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        
        return df[["date", "fighter_name", "search_interest"]]
    
    except Exception as e:
        logger.warning(f"Failed to fetch trends for {fighter_name}: {e}")
        return pd.DataFrame()


def fetch_trends_batch(fighter_names, start_date, end_date, delay_seconds=2.0):
    # Fetch Google Trends for multiple fighters with rate limiting
    # Note: Google Trends has strict rate limits, so this is slow
    logger.info(f"Fetching trends for {len(fighter_names)} fighters...")
    
    pytrends = create_pytrends_client()
    if pytrends is None:
        logger.error("Cannot create pytrends client")
        return pd.DataFrame()
    
    all_data = []
    
    for i, fighter in enumerate(fighter_names):
        df = fetch_fighter_trends(fighter, start_date, end_date, pytrends)
        
        if not df.empty:
            all_data.append(df)
        
        if (i + 1) % 10 == 0:
            logger.info(f"  Fetched {i + 1}/{len(fighter_names)} fighters")
        
        # Rate limiting important
        time.sleep(delay_seconds + random.uniform(0, 1))
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"Fetched {len(result)} total trend records")
        return result
    
    return pd.DataFrame()


def generate_synthetic_trends(fighter_names, events_df, start_year=2015, end_year=2025):
    # Generate synthetic Google Trends data for fighters
    # Creates realistic search interest patterns with spikes around fight dates
    logger.info(f"Generating synthetic trends for {len(fighter_names)} fighters...")
    
    all_records = []
    
    # Create date range
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    date_range = pd.date_range(start_date, end_date, freq='W')  # Weekly data
    
    # Get event dates if available
    event_dates = set()
    if events_df is not None and "event_date" in events_df.columns:
        event_dates = set(pd.to_datetime(events_df["event_date"]).dropna())
    
    for fighter_idx, fighter in enumerate(fighter_names):
        # Base interest level varies by star power simulated
        base_interest = random.randint(5, 40)
        
        # Add some randomness to make each fighter unique
        interest_volatility = random.uniform(0.1, 0.5)
        
        for date in date_range:
            # Base interest with random walk
            search_interest = base_interest + random.gauss(0, base_interest * interest_volatility)
            
            # Spike around events simulate pre-fight buzz
            for event_date in event_dates:
                days_to_event = (event_date - date).days
                
                # Pre-event spike 7 days before
                if 0 <= days_to_event <= 7:
                    spike = random.uniform(1.5, 3.0) * (7 - days_to_event) / 7
                    search_interest *= (1 + spike)
                
                # Post-event decay 14 days after
                elif -14 <= days_to_event < 0:
                    decay = random.uniform(0.8, 1.0)
                    search_interest *= decay
            
            # Ensure reasonable bounds
            search_interest = max(0, min(100, search_interest))
            
            all_records.append({
                "date": date,
                "fighter_name": fighter,
                "search_interest": int(search_interest)
            })
        
        # Progress
        if (fighter_idx + 1) % 100 == 0:
            logger.info(f"  Generated trends for {fighter_idx + 1}/{len(fighter_names)} fighters")
    
    df = pd.DataFrame(all_records)
    logger.info(f"Generated {len(df)} synthetic trend records")
    
    return df


def calculate_pre_event_buzz(trends_df, fights_df):
    # Calculate pre-event buzz metrics for each fight
    logger.info("Calculating pre-event buzz metrics...")
    
    if trends_df.empty or fights_df.empty:
        return pd.DataFrame()
    
    trends_df["date"] = pd.to_datetime(trends_df["date"])
    
    # Find fighter name columns
    f1_col = None
    f2_col = None
    
    if "fighter1_name" in fights_df.columns:
        f1_col = "fighter1_name"
    elif "fighter1" in fights_df.columns:
        f1_col = "fighter1"
    if f2_col is None:
        f2_col = "fighter2_name" if "fighter2_name" in fights_df.columns else "fighter2"
    
    # Handle bout column
    if f1_col not in fights_df.columns and "bout" in fights_df.columns:
        fights_df = fights_df.copy()
        fights_df["fighter1_name"] = fights_df["bout"].str.extract(r"^(.+?)\s+vs\.?\s+")[0].str.strip()
        fights_df["fighter2_name"] = fights_df["bout"].str.extract(r"\s+vs\.?\s+(.+)$")[0].str.strip()
        f1_col = "fighter1_name"
        f2_col = "fighter2_name"
    
    buzz_records = []
    
    for _, fight in fights_df.iterrows():
        fighter1 = fight.get(f1_col)
        fighter2 = fight.get(f2_col)
        
        # Get event date if available
        event_date = None
        if "event_date" in fight:
            try:
                event_date = pd.to_datetime(fight["event_date"])
            except:
                pass
        elif "date" in fight:
            try:
                event_date = pd.to_datetime(fight["date"])
            except:
                pass
        
        if event_date is None:
            # Use a random recent date
            event_date = datetime.now() - timedelta(days=random.randint(1, 365))
        
        record = {
            "fighter1_name": fighter1,
            "fighter2_name": fighter2,
            "event_date": event_date
        }
        
        # Calculate buzz for both fighters
        for fighter_key, fighter_name in [("f1", fighter1), ("f2", fighter2)]:
            fighter_trends = trends_df[trends_df["fighter_name"] == fighter_name]
            
            if fighter_trends.empty:
                # Default values
                record[f"{fighter_key}_avg_search_7d"] = 25
                record[f"{fighter_key}_avg_search_30d"] = 25
                record[f"{fighter_key}_peak_search_7d"] = 30
                record[f"{fighter_key}_trend_direction"] = 0
            else:
                # Filter to pre-event period
                week_before = fighter_trends[
                    (fighter_trends["date"] <= event_date) &
                    (fighter_trends["date"] >= event_date - timedelta(days=7))
                ]
                month_before = fighter_trends[
                    (fighter_trends["date"] <= event_date) &
                    (fighter_trends["date"] >= event_date - timedelta(days=30))
                ]
                
                # Calculate metrics
                if not week_before.empty:
                    record[f"{fighter_key}_avg_search_7d"] = week_before["search_interest"].mean()
                    record[f"{fighter_key}_peak_search_7d"] = week_before["search_interest"].max()
                else:
                    record[f"{fighter_key}_avg_search_7d"] = 25
                    record[f"{fighter_key}_peak_search_7d"] = 30
                
                if not month_before.empty:
                    record[f"{fighter_key}_avg_search_30d"] = month_before["search_interest"].mean()
                    
                    # Trend direction: compare first half vs second half of 30 days
                    if len(month_before) >= 4:
                        first_half = month_before.iloc[:len(month_before)//2]["search_interest"].mean()
                        second_half = month_before.iloc[len(month_before)//2:]["search_interest"].mean()
                        record[f"{fighter_key}_trend_direction"] = 1 if second_half > first_half else -1
                    else:
                        record[f"{fighter_key}_trend_direction"] = 0
                else:
                    record[f"{fighter_key}_avg_search_30d"] = 25
                    record[f"{fighter_key}_trend_direction"] = 0
        
        # Aggregate metrics for the fight
        record["combined_buzz_7d"] = record["f1_avg_search_7d"] + record["f2_avg_search_7d"]
        record["combined_buzz_30d"] = record["f1_avg_search_30d"] + record["f2_avg_search_30d"]
        record["max_peak_search"] = max(record["f1_peak_search_7d"], record["f2_peak_search_7d"])
        record["buzz_differential"] = abs(record["f1_avg_search_7d"] - record["f2_avg_search_7d"])
        
        buzz_records.append(record)
    
    df = pd.DataFrame(buzz_records)
    logger.info(f"Calculated buzz metrics for {len(df)} fights")
    
    return df


def run_trends_pipeline(data_dir, output_dir, use_synthetic=True, max_fighters=100):
    # Main function to run the Google Trends pipeline
    logger.info("=" * 60)
    logger.info("UFC Google Trends Pipeline")
    logger.info("=" * 60)
    
    # Output paths
    trends_path = os.path.join(output_dir, "external", "google_trends.csv")
    buzz_path = os.path.join(output_dir, "external", "fighter_buzz.csv")
    os.makedirs(os.path.dirname(trends_path), exist_ok=True)
    
    # Load fight data to get fighter names
    fights_path = os.path.join(data_dir, "raw", "fight_results.csv")
    if not os.path.exists(fights_path):
        logger.error(f"Fight data not found at {fights_path}")
        return
    
    fights_df = pd.read_csv(fights_path)
    # Lowercase columns
    fights_df.columns = [c.lower() for c in fights_df.columns]
    
    # Extract unique fighter names
    fighter_names = set()
    
    # Handle different column formats
    if "bout" in fights_df.columns:
        # Parse "Fighter1 vs. Fighter2" format
        for bout in fights_df["bout"].dropna():
            parts = bout.split(" vs")
            if len(parts) >= 2:
                fighter_names.add(parts[0].strip())
                fighter_names.add(parts[1].strip().lstrip(". "))
    else:
        # Try common column names
        if "fighter1_name" in fights_df.columns:
            fighter_names.update(fights_df["fighter1_name"].dropna().unique())
        elif "fighter1" in fights_df.columns:
            fighter_names.update(fights_df["fighter1"].dropna().unique())
        
        if "fighter2_name" in fights_df.columns:
            fighter_names.update(fights_df["fighter2_name"].dropna().unique())
        elif "fighter2" in fights_df.columns:
            fighter_names.update(fights_df["fighter2"].dropna().unique())
    
    fighter_names = list(fighter_names)
    logger.info(f"Found {len(fighter_names)} unique fighters")
    
    # Load events for dates
    events_df = None
    events_path = os.path.join(data_dir, "raw", "events.csv")
    if os.path.exists(events_path):
        events_df = pd.read_csv(events_path)
        events_df.columns = [c.lower() for c in events_df.columns]
        # Parse dates
        if "event_date" in events_df.columns:
            events_df["event_date"] = pd.to_datetime(events_df["event_date"], errors="coerce")
        elif "date" in events_df.columns:
            events_df["event_date"] = pd.to_datetime(events_df["date"], errors="coerce")
    
    # Check if we already have trends data
    trends_df = None
    if os.path.exists(trends_path):
        logger.info(f"Loading existing trends from {trends_path}")
        trends_df = pd.read_csv(trends_path)
        logger.info(f"Loaded {len(trends_df)} existing trend records")
    
    # Fetch or generate trends
    if trends_df is None or len(trends_df) == 0:
        if use_synthetic or not PYTRENDS_AVAILABLE:
            # Generate synthetic data
            trends_df = generate_synthetic_trends(
                fighter_names[:max_fighters] if len(fighter_names) > max_fighters else fighter_names,
                events_df
            )
        else:
            # Fetch real data very slow due to rate limits
            trends_df = fetch_trends_batch(
                fighter_names[:max_fighters],
                start_date="2020-01-01",
                end_date="2025-01-01"
            )
    
    if trends_df.empty:
        logger.error("Failed to obtain trends data!")
        return
    
    # Save raw trends
    trends_df.to_csv(trends_path, index=False)
    logger.info(f"Saved {len(trends_df)} trend records to {trends_path}")
    
    # Calculate pre-event buzz
    buzz_df = calculate_pre_event_buzz(trends_df, fights_df)
    
    if not buzz_df.empty:
        buzz_df.to_csv(buzz_path, index=False)
        logger.info(f"Saved {len(buzz_df)} buzz records to {buzz_path}")
    
    # Print summary
    logger.info("\nGoogle Trends Summary:")
    logger.info(f"  Total trend records: {len(trends_df)}")
    logger.info(f"  Unique fighters: {trends_df['fighter_name'].nunique()}")
    logger.info(f"  Date range: {trends_df['date'].min()} to {trends_df['date'].max()}")
    if not buzz_df.empty:
        logger.info(f"  Buzz records: {len(buzz_df)}")
        logger.info(f"  Avg combined buzz (7d): {buzz_df['combined_buzz_7d'].mean():.1f}")
    
    logger.info("\n" + "=" * 60)
    logger.info("Google Trends Pipeline Complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UFC Google Trends Fetcher")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    parser.add_argument("--real-api", action="store_true", help="Use real Google Trends API (slow)")
    parser.add_argument("--max-fighters", type=int, default=500, help="Max fighters to process")
    
    args = parser.parse_args()
    
    run_trends_pipeline(
        args.data_dir,
        args.output_dir,
        use_synthetic=not args.real_api,
        max_fighters=args.max_fighters
    )

