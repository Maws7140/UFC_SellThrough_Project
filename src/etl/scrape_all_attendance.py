# Scrape all attendance from Wikipedia
# DS 410 project

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime
import argparse
import os

HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

REQUEST_DELAY = 1.5


def parse_number(text):
    # Parse number from text
    if not text:
        return None
    
    # Remove footnote references like [1], [2], etc.
    text = re.sub(r'\[\d+\]', '', text)
    
    # Remove common prefixes/suffixes
    text = text.replace('~', '').replace('+', '').replace('*', '')
    
    # Find numbers with commas attendance is usually 4+ digits
    number_match = re.search(r'[\d,]+', text)
    if number_match:
        number_str = number_match.group().replace(',', '')
        try:
            num = int(number_str)
            # Attendance should be at least 100 filter out footnote numbers
            if num >= 100:
                return num
            return None
        except ValueError:
            return None
    return None


def parse_currency(text):
    # Parse money
    if not text:
        return None
    text = re.sub(r'[US\$]', '', text).strip()
    if 'million' in text.lower():
        number_match = re.search(r'[\d.]+', text)
        if number_match:
            try:
                return float(number_match.group()) * 1_000_000
            except ValueError:
                return None
    number_match = re.search(r'[\d,]+\.?\d*', text)
    if number_match:
        number_str = number_match.group().replace(',', '')
        try:
            return float(number_str)
        except ValueError:
            return None
    return None


def event_name_to_wiki_url(event_name):
    # Convert event name to Wikipedia URL
    # Clean up the event name
    wiki_name = event_name.strip()
    
    # Replace spaces with underscores
    wiki_name = wiki_name.replace(' ', '_')
    
    # URL encode special characters
    wiki_name = wiki_name.replace(':', '%3A')
    
    return f"https://en.wikipedia.org/wiki/{wiki_name}"


def scrape_event_page(url, event_name):
    # Scrape one event page
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        
        # If 404, try alternate URL formats
        if response.status_code == 404:
            # Try without special characters
            alt_name = re.sub(r'[:\-]', '', event_name).replace('  ', ' ')
            alt_url = event_name_to_wiki_url(alt_name)
            response = requests.get(alt_url, headers=HEADERS, timeout=30)
        
        if response.status_code == 404:
            return None
            
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        result = {
            'event_name': event_name,
            'wikipedia_url': url,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Find the infobox
        infobox = soup.find('table', class_='infobox')
        if not infobox:
            infobox = soup.find('table', class_='infobox vevent')
        
        if not infobox:
            return None
        
        # Parse infobox rows
        for row in infobox.find_all('tr'):
            header = row.find('th')
            data = row.find('td')
            
            if not header or not data:
                continue
            
            header_text = header.get_text(strip=True).lower()
            data_text = data.get_text(strip=True)
            
            if 'attendance' in header_text:
                result['attendance'] = parse_number(data_text)
                result['attendance_raw'] = data_text
            
            elif 'gate' in header_text:
                result['gate_revenue'] = parse_currency(data_text)
                result['gate_raw'] = data_text
            
            elif 'venue' in header_text:
                result['venue'] = data_text
                venue_link = data.find('a')
                if venue_link and venue_link.get('href'):
                    result['venue_wiki_path'] = venue_link.get('href')
            
            elif 'city' in header_text or 'location' in header_text:
                result['location'] = data_text
            
            elif 'date' in header_text:
                result['date_raw'] = data_text
                for fmt in ['%B %d, %Y', '%d %B %Y', '%Y-%m-%d']:
                    try:
                        date_str = data_text.split('[')[0].strip()
                        # Handle non-breaking spaces
                        date_str = date_str.replace('\xa0', ' ')
                        result['event_date'] = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue
            
            elif 'buyrate' in header_text.replace(' ', '') or 'buys' in header_text:
                result['ppv_buys'] = parse_number(data_text)
        
        # Only return if we have attendance or gate data
        return result if result.get('attendance') or result.get('gate_revenue') else None
        
    except requests.exceptions.RequestException as e:
        return None
    except Exception as e:
        return None


def scrape_venue_capacity(venue_wiki_path):
    # Get venue capacity
    if not venue_wiki_path or not venue_wiki_path.startswith('/wiki/'):
        return None
    
    url = f"https://en.wikipedia.org{venue_wiki_path}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        infobox = soup.find('table', class_='infobox')
        if not infobox:
            return None
        
        for row in infobox.find_all('tr'):
            header = row.find('th')
            data = row.find('td')
            
            if header and data:
                header_text = header.get_text(strip=True).lower()
                
                if 'capacity' in header_text:
                    data_text = data.get_text(strip=True)
                    numbers = re.findall(r'[\d,]+', data_text)
                    if numbers:
                        capacities = [int(n.replace(',', '')) for n in numbers]
                        return max(capacities)
        
        return None
        
    except Exception as e:
        return None


def scrape_all_historical_events(events_path, output_path, limit=None, resume=True):
    # Scrape all events from CSV
    print("Loading events...")
    
    events_df = pd.read_csv(events_path)
    print(f"Found {len(events_df)} events")
    
    # Load existing data if resuming
    existing_data = []
    scraped_events = set()
    
    if resume and os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        existing_data = existing_df.to_dict('records')
        scraped_events = set(existing_df['event_name'].dropna())
        print(f"Loaded {len(existing_data)} existing records")
    
    # Filter events to scrape
    events_to_scrape = events_df[~events_df['EVENT'].isin(scraped_events)]
    
    if limit:
        events_to_scrape = events_to_scrape.head(limit)
    
    print(f"Scraping {len(events_to_scrape)} new events...")
    
    all_data = existing_data.copy()
    new_count = 0
    success_count = 0
    
    for i, row in events_to_scrape.iterrows():
        event_name = row['EVENT']
        
        # Skip future events no attendance data
        try:
            event_date = pd.to_datetime(row['DATE'])
            if event_date > datetime.now():
                continue
        except:
            pass
        
        wiki_url = event_name_to_wiki_url(event_name)
        
        print(f"[{new_count+1}] Scraping: {event_name}")
        
        data = scrape_event_page(wiki_url, event_name)
        
        if data:
            # Get venue capacity
            if data.get('venue_wiki_path'):
                time.sleep(REQUEST_DELAY / 2)
                capacity = scrape_venue_capacity(data['venue_wiki_path'])
                if capacity:
                    data['venue_capacity'] = capacity
                    
                    # Calculate sell-through
                    if data.get('attendance'):
                        data['sell_through'] = data['attendance'] / capacity
            
            data['source'] = 'Wikipedia'
            all_data.append(data)
            success_count += 1
        
        new_count += 1
        
        # Save periodically
        if new_count % 20 == 0:
            df = pd.DataFrame(all_data)
            df.to_csv(output_path, index=False)
            print(f"Progress: {new_count} processed, {success_count} with data")
        
        time.sleep(REQUEST_DELAY)
    
    # Final save
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        # Sort by date
        if 'event_date' in df.columns:
            df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
            df = df.sort_values('event_date', ascending=False)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
    
    # Print summary
    print("\n" + "="*60)
    print("SCRAPING SUMMARY")
    print("="*60)
    print(f"Total events in database: {len(events_df)}")
    print(f"Events processed this run: {new_count}")
    print(f"Events with attendance data: {success_count}")
    print(f"Total events in output: {len(df)}")
    
    if not df.empty and 'attendance' in df.columns:
        print(f"\nEvents with attendance: {df['attendance'].notna().sum()}")
        print(f"Events with gate revenue: {df['gate_revenue'].notna().sum() if 'gate_revenue' in df.columns else 0}")
        print(f"Events with sell-through: {df['sell_through'].notna().sum() if 'sell_through' in df.columns else 0}")
        
        if df['attendance'].notna().any():
            print(f"\nAttendance range: {df['attendance'].min():,.0f} - {df['attendance'].max():,.0f}")
    
    print(f"\nOutput saved to: {output_path}")
    print("="*60)
    
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape all UFC attendance data from Wikipedia")
    parser.add_argument("--events", default="./data/raw/events.csv",
                        help="Path to events CSV")
    parser.add_argument("--output", default="./data/external/attendance_full.csv",
                        help="Output CSV path")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of events (for testing)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh instead of resuming")
    
    args = parser.parse_args()
    
    scrape_all_historical_events(
        events_path=args.events,
        output_path=args.output,
        limit=args.limit,
        resume=not args.no_resume
    )

