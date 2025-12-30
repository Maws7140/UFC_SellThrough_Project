import os
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


UFCSTATS_GITHUB = {
    "events": "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_events.csv",
    "fight_results": "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_results.csv",
    "fight_stats": "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_stats.csv",
    "fight_details": "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fight_details.csv",
    "fighter_details": "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fighter_details.csv",
    "fighter_tott": "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/ufc_fighter_tott.csv",
}

NAGER_DATE_API = "https://date.nager.at/api/v3/PublicHolidays"


def download_file(url, output_path, force=False):
    if os.path.exists(output_path) and not force:
        logger.info(f"File exists: {output_path}")
        return output_path
    
    logger.info(f"Downloading {url}...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'wb') as f:
        f.write(response.content)
    
    logger.info(f"Saved to {output_path}")
    return output_path


def download_all_ufcstats(data_dir, force=False):
    paths = {}
    failed_downloads = []
    
    for name, url in UFCSTATS_GITHUB.items():
        output_path = os.path.join(data_dir, "raw", f"{name}.csv")
        try:
            paths[name] = download_file(url, output_path, force)
        except Exception as e:
            logger.warning(f"Failed to download {name}: {e}")
            failed_downloads.append(name)
            
            if os.path.exists(output_path):
                logger.info(f"Using existing file: {output_path}")
                paths[name] = output_path
    
    if failed_downloads:
        logger.warning("=" * 60)
        logger.warning("NETWORK ISSUES DETECTED")
        logger.warning("=" * 60)
        logger.warning(f"Failed to download: {', '.join(failed_downloads)}")
        logger.warning("")
        logger.warning("SOLUTION: Download data manually from:")
        logger.warning("  https://github.com/Greco1899/scrape_ufc_stats")
        logger.warning("  Click 'Code' > 'Download ZIP'")
        logger.warning("  Extract CSV files to: data/raw/")
        logger.warning("=" * 60)
    
    return paths


def get_holidays(year, country_code="US"):
    url = f"{NAGER_DATE_API}/{year}/{country_code}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        holidays = response.json()
        df = pd.DataFrame(holidays)
        df['date'] = pd.to_datetime(df['date'])
        return df[['date', 'localName', 'name', 'types']]
    except Exception as e:
        logger.warning(f"Failed to get holidays for {year}/{country_code}: {e}")
        return pd.DataFrame()


def get_holiday_calendar(start_year, end_year):
    all_holidays = []
    for year in range(start_year, end_year + 1):
        for country in ["US", "CA", "GB"]:
            df = get_holidays(year, country)
            if not df.empty:
                df['country'] = country
                all_holidays.append(df)
    
    if all_holidays:
        return pd.concat(all_holidays, ignore_index=True)
    return pd.DataFrame()


def run_ingestion(data_dir, force=False):
    logger.info("Starting data ingestion...")
    
    paths = {}
    
    logger.info("Downloading UFCStats data...")
    ufc_paths = download_all_ufcstats(data_dir, force)
    paths.update(ufc_paths)
    
    logger.info("Fetching holiday calendar...")
    holidays_path = os.path.join(data_dir, "external", "holidays.csv")
    if not os.path.exists(holidays_path) or force:
        os.makedirs(os.path.dirname(holidays_path), exist_ok=True)
        holidays_df = get_holiday_calendar(2010, 2025)
        if not holidays_df.empty:
            holidays_df.to_csv(holidays_path, index=False)
    paths['holidays'] = holidays_path
    
    logger.info("Data ingestion complete!")
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UFC data ingestion")
    parser.add_argument("--data-dir", default="./data")
    parser.add_argument("--force", action="store_true")
    
    args = parser.parse_args()
    
    paths = run_ingestion(args.data_dir, args.force)
    print("\nDownloaded files:")
    for name, path in paths.items():
        print(f"  {name}: {path}")
