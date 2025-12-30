# Scrape betting odds
# DS 410 project

import os
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import argparse

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

BFO_BASE_URL = "https://www.bestfightodds.com"
KAGGLE_ODDS_URL = "https://raw.githubusercontent.com/jasonchanhku/UFC-MMA-Predictor/master/data/ufc_odds.csv"


def american_to_decimal(american_odds):
    if american_odds > 0:
        return (american_odds / 100) + 1
    else:
        return (100 / abs(american_odds)) + 1


def decimal_to_implied_probability(decimal_odds):
    if decimal_odds <= 0:
        return 0.0
    return 1 / decimal_odds


def american_to_implied_probability(american_odds):
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def scrape_event_odds(event_url):
    print(f"Scraping odds from: {event_url}")
    
    try:
        response = requests.get(event_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {event_url}: {e}")
        return []
    
    soup = BeautifulSoup(response.content, "lxml")
    fights = []
    
    fight_tables = soup.find_all("table", class_="odds-table")
    
    for table in fight_tables:
        try:
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            
            # First two rows are typically the fighters
            fighter1_row = rows[0]
            fighter2_row = rows[1]
            
            # Extract fighter names
            f1_name_elem = fighter1_row.find("th", class_="oppcell")
            f2_name_elem = fighter2_row.find("th", class_="oppcell")
            
            if not f1_name_elem or not f2_name_elem:
                continue
            
            fighter1_name = f1_name_elem.get_text(strip=True)
            fighter2_name = f2_name_elem.get_text(strip=True)
            
            # Extract odds first sportsbook column
            f1_odds_elem = fighter1_row.find("td", class_="odds")
            f2_odds_elem = fighter2_row.find("td", class_="odds")
            
            if f1_odds_elem and f2_odds_elem:
                try:
                    f1_odds = int(f1_odds_elem.get_text(strip=True).replace("+", ""))
                    f2_odds = int(f2_odds_elem.get_text(strip=True).replace("+", ""))
                except ValueError:
                    f1_odds = 0
                    f2_odds = 0
                
                fights.append({
                    "fighter1_name": fighter1_name,
                    "fighter2_name": fighter2_name,
                    "fighter1_odds": f1_odds,
                    "fighter2_odds": f2_odds,
                    "fighter1_implied_prob": american_to_implied_probability(f1_odds) if f1_odds != 0 else 0.5,
                    "fighter2_implied_prob": american_to_implied_probability(f2_odds) if f2_odds != 0 else 0.5,
                })
        except Exception as e:
            pass
            continue
    
    return fights


def scrape_bfo_events(max_events=100, delay=2.0):
    # Scrape events from BestFightOdds
    print(f"Scraping up to {max_events} events...")
    
    all_fights = []
    
    try:
        # Get main page with events list
        response = requests.get(f"{BFO_BASE_URL}/events", headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "lxml")
        
        # Find event links
        event_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/events/" in href and href not in event_links:
                event_links.append(href)
        
        print(f"Found {len(event_links)} event links")
        
        # Scrape each event
        for i, event_link in enumerate(event_links[:max_events]):
            if not event_link.startswith("http"):
                event_url = BFO_BASE_URL + event_link
            else:
                event_url = event_link
            
            fights = scrape_event_odds(event_url)
            
            # Add event info to each fight
            event_name = event_link.split("/")[-1].replace("-", " ").title()
            for fight in fights:
                fight["event_name"] = event_name
                fight["source"] = "BestFightOdds"
            
            all_fights.extend(fights)
            
            # Progress
            if (i + 1) % 10 == 0:
                print(f"  Scraped {i + 1}/{min(max_events, len(event_links))} events")
            
            # Rate limiting
            time.sleep(delay + random.uniform(0, 1))
    
    except Exception as e:
        print(f"Error scraping BFO: {e}")
    
    df = pd.DataFrame(all_fights)
    print(f"Scraped {len(df)} total fight odds records")
    
    return df


def fetch_kaggle_odds():
    # Get odds from Kaggle dataset
    print("Fetching odds from Kaggle...")
    
    try:
        df = pd.read_csv(KAGGLE_ODDS_URL)
        print(f"Loaded {len(df)} odds records from Kaggle")
        return df
    except Exception as e:
        print(f"Failed to fetch Kaggle data: {e}")
        return pd.DataFrame()


def generate_synthetic_odds(fights_df):
    # Generate fake odds for training
    print("Generating synthetic betting odds...")
    
    # Find fighter name columns
    f1_col = None
    f2_col = None
    
    if "fighter1_name" in fights_df.columns:
        f1_col = "fighter1_name"
    elif "fighter1" in fights_df.columns:
        f1_col = "fighter1"
    
    if "fighter2_name" in fights_df.columns:
        f2_col = "fighter2_name"
    elif "fighter2" in fights_df.columns:
        f2_col = "fighter2"
    
    # Handle bout column
    if f1_col not in fights_df.columns and "bout" in fights_df.columns:
        fights_df = fights_df.copy()
        fights_df["fighter1_name"] = fights_df["bout"].str.extract(r"^(.+?)\s+vs\.?\s+")[0].str.strip()
        fights_df["fighter2_name"] = fights_df["bout"].str.extract(r"\s+vs\.?\s+(.+)$")[0].str.strip()
        f1_col = "fighter1_name"
        f2_col = "fighter2_name"
    
    odds_records = []
    
    for _, row in fights_df.iterrows():
        fighter1 = row.get(f1_col, "Fighter1")
        fighter2 = row.get(f2_col, "Fighter2")
        
        # Generate realistic odds based on random matchup quality
        # Favorites typically range from -150 to -500
        # Underdogs typically range from +130 to +400
        
        # Randomly decide who's the favorite
        if random.random() > 0.5:
            favorite = fighter1
            underdog = fighter2
        else:
            favorite = fighter2
            underdog = fighter1
        
        # Generate favorite odds (negative)
        favorite_odds = -random.randint(130, 400)
        
        # Calculate underdog odds to create realistic juice ~10% vig
        fav_prob = american_to_implied_probability(favorite_odds)
        underdog_prob = 1 - fav_prob + 0.05  # Add ~5% juice
        
        # Convert back to American
        if underdog_prob < 0.5:
            underdog_odds = int((100 / underdog_prob) - 100)
        else:
            underdog_odds = int(-100 / (underdog_prob - 1) - 100)
        
        # Ensure underdog odds are positive
        underdog_odds = abs(underdog_odds)
        
        # Create record
        record = {
            "fighter1_name": fighter1,
            "fighter2_name": fighter2,
            "fighter1_odds": favorite_odds if favorite == fighter1 else underdog_odds,
            "fighter2_odds": underdog_odds if favorite == fighter1 else favorite_odds,
        }
        
        # Add implied probabilities
        record["fighter1_implied_prob"] = american_to_implied_probability(record["fighter1_odds"])
        record["fighter2_implied_prob"] = american_to_implied_probability(record["fighter2_odds"])
        
        # Add opening odds (slightly different from closing)
        line_movement = random.randint(-30, 30)
        record["opening_odds_f1"] = record["fighter1_odds"] + line_movement
        record["opening_odds_f2"] = record["fighter2_odds"] - line_movement
        
        # Calculate line movement
        record["line_movement_f1"] = record["fighter1_odds"] - record["opening_odds_f1"]
        record["line_movement_f2"] = record["fighter2_odds"] - record["opening_odds_f2"]
        
        # Add spread absolute difference in implied probabilities
        record["odds_spread"] = abs(record["fighter1_implied_prob"] - record["fighter2_implied_prob"])
        
        # Add event info if available
        if "event" in row:
            record["event_name"] = row["event"]
        elif "event_name" in row:
            record["event_name"] = row["event_name"]
        
        record["source"] = "Synthetic"
        
        odds_records.append(record)
    
    df = pd.DataFrame(odds_records)
    print(f"Generated {len(df)} synthetic odds records")
    
    return df


def calculate_betting_features(odds_df):
    # Calculate derived betting features from raw odds
    print("Calculating betting features...")
    
    df = odds_df.copy()
    
    # Ensure required columns exist
    if "fighter1_odds" not in df.columns or "fighter2_odds" not in df.columns:
        print("Missing odds columns, cannot calculate features")
        return df
    
    # Implied probabilities (if not already calculated)
    if "fighter1_implied_prob" not in df.columns:
        df["fighter1_implied_prob"] = df["fighter1_odds"].apply(american_to_implied_probability)
    if "fighter2_implied_prob" not in df.columns:
        df["fighter2_implied_prob"] = df["fighter2_odds"].apply(american_to_implied_probability)
    
    # Favorite indicator
    df["f1_is_favorite"] = (df["fighter1_implied_prob"] > df["fighter2_implied_prob"]).astype(int)
    
    # Odds spread measure of how lopsided the matchup is
    df["odds_spread"] = abs(df["fighter1_implied_prob"] - df["fighter2_implied_prob"])
    
    # Competitive matchup flag (spread < 10%)
    df["is_competitive_matchup"] = (df["odds_spread"] < 0.10).astype(int)
    
    # Heavy favorite flag one fighter > 70% implied probability
    df["has_heavy_favorite"] = (
        (df["fighter1_implied_prob"] > 0.70) | (df["fighter2_implied_prob"] > 0.70)
    ).astype(int)
    
    # Line movement features if available
    if "line_movement_f1" in df.columns:
        df["big_line_movement"] = (abs(df["line_movement_f1"]) > 50).astype(int)
    
    print(f"Calculated betting features for {len(df)} records")
    
    return df


def run_betting_scraper(data_dir, output_dir, use_synthetic=True):
    # Main betting odds scraper
    print("=" * 60)
    print("UFC Betting Odds Pipeline")
    print("=" * 60)
    
    output_path = os.path.join(output_dir, "external", "betting_odds.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    odds_df = None
    
    # Check if we already have odds data
    if os.path.exists(output_path):
        print(f"Loading existing odds from {output_path}")
        odds_df = pd.read_csv(output_path)
        print(f"Loaded {len(odds_df)} existing odds records")
    
    # If no data, try Kaggle first fastest
    if odds_df is None or len(odds_df) == 0:
        odds_df = fetch_kaggle_odds()
    
    # If still no data and synthetic is enabled, generate synthetic odds
    if (odds_df is None or len(odds_df) == 0) and use_synthetic:
        # Load fight data to generate odds
        fights_path = os.path.join(data_dir, "raw", "fight_results.csv")
        if os.path.exists(fights_path):
            print(f"Loading fights from {fights_path}")
            fights_df = pd.read_csv(fights_path)
            # Lowercase columns
            fights_df.columns = [c.lower() for c in fights_df.columns]
            odds_df = generate_synthetic_odds(fights_df)
        else:
            print("No fight data available to generate synthetic odds")
            return
    
    if odds_df is None or len(odds_df) == 0:
        print("Failed to obtain any betting odds data!")
        return
    
    # Calculate betting features
    odds_df = calculate_betting_features(odds_df)
    
    # Save
    odds_df.to_csv(output_path, index=False)
    print(f"Saved {len(odds_df)} odds records to {output_path}")
    
    print("\nBetting Odds Summary:")
    print(f"  Total records: {len(odds_df)}")
    if "source" in odds_df.columns:
        print(f"  Sources: {odds_df['source'].value_counts().to_dict()}")
    if "odds_spread" in odds_df.columns:
        print(f"  Avg odds spread: {odds_df['odds_spread'].mean():.3f}")
    if "is_competitive_matchup" in odds_df.columns:
        print(f"  Competitive matchups: {odds_df['is_competitive_matchup'].sum()}")
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UFC Betting Odds Scraper")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    parser.add_argument("--scrape", action="store_true", help="Scrape from BestFightOdds (slow)")
    parser.add_argument("--max-events", type=int, default=50, help="Max events to scrape")
    
    args = parser.parse_args()
    
    run_betting_scraper(args.data_dir, args.output_dir, use_synthetic=True)

