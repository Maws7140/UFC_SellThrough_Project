import pandas as pd
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))
from scrape_attendance import scrape_event_page


def load_sample_data(sample_path):
    if not os.path.exists(sample_path):
        raise FileNotFoundError(f"Sample data not found: {sample_path}")
    
    df = pd.read_csv(sample_path)
    print(f"Loaded {len(df)} events from sample data")
    return df


def scrape_specific_events(event_urls: list) -> pd.DataFrame:
    # Scrape specific UFC events by URL
    import time
    from scrape_attendance import REQUEST_DELAY
    
    results = []
    
    for i, url in enumerate(event_urls):
        print(f"[{i+1}/{len(event_urls)}] Scraping: {url}")
        data = scrape_event_page(url)
        
        if data:
            # Calculate sell-through if we have attendance and capacity
            if data.get('attendance') and data.get('venue_capacity'):
                data['sell_through'] = data['attendance'] / data['venue_capacity']
            else:
                data['sell_through'] = None
            
            # Add source
            data['source'] = 'Wikipedia'
            results.append(data)
        
        time.sleep(REQUEST_DELAY)
    
    if results:
        return pd.DataFrame(results)
    return pd.DataFrame()


def merge_attendance_data(sample_path, output_path, additional_urls=None):
    # Merge sample data with any additional scraped data
    df_sample = load_sample_data(sample_path)
    
    # Scrape additional events if provided
    df_additional = pd.DataFrame()
    if additional_urls:
        print(f"\nScraping {len(additional_urls)} additional events...")
        df_additional = scrape_specific_events(additional_urls)
        if not df_additional.empty:
            print(f"Scraped {len(df_additional)} additional events")
    
    # Merge
    if not df_additional.empty:
        # Standardize column names
        df_sample['wikipedia_url'] = None  # Sample doesn't have URLs
        df_additional['event_name'] = df_additional.get('event_name', '')
        df_additional['event_date'] = df_additional.get('event_date', '')
        
        # Combine
        df_merged = pd.concat([df_sample, df_additional], ignore_index=True)
        
        # Remove duplicates based on event_name and event_date
        df_merged = df_merged.drop_duplicates(
            subset=['event_name', 'event_date'],
            keep='first'
        )
    else:
        df_merged = df_sample
    
    # Sort by date
    df_merged['event_date'] = pd.to_datetime(df_merged['event_date'], errors='coerce')
    df_merged = df_merged.sort_values('event_date', ascending=False)
    
    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_merged.to_csv(output_path, index=False)
    
    print(f"\n{'='*60}")
    print("ATTENDANCE DATA SUMMARY")
    print(f"{'='*60}")
    print(f"Total events: {len(df_merged)}")
    print(f"Events with attendance: {df_merged['attendance'].notna().sum()}")
    print(f"Events with sell-through: {df_merged['sell_through'].notna().sum()}")
    print(f"Date range: {df_merged['event_date'].min()} to {df_merged['event_date'].max()}")
    print(f"\nSaved to: {output_path}")
    print(f"{'='*60}")
    
    return df_merged


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge attendance data")
    parser.add_argument("--sample", 
                       default="./data/external/ufc_attendance_sample.csv",
                       help="Path to sample attendance CSV")
    parser.add_argument("--output",
                       default="./data/external/attendance.csv",
                       help="Output path for merged data")
    parser.add_argument("--add-events",
                       nargs="+",
                       help="Additional Wikipedia URLs to scrape (optional)")
    
    args = parser.parse_args()
    
    merge_attendance_data(
        sample_path=args.sample,
        output_path=args.output,
        additional_urls=args.add_events
    )

