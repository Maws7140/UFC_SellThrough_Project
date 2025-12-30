# Scrape attendance from Wikipedia UFC pages
# DS 410 project

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from datetime import datetime
import argparse
import os

# Headers for requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

# Wait between requests
REQUEST_DELAY = 1.0


def parse_number(text):
    # Parse number from text, remove commas etc
    if not text:
        return None
    
    # Remove common prefixes/suffixes
    text = text.replace('~', '').replace('+', '').replace('*', '')
    
    # Extract digits and commas
    number_match = re.search(r'[\d,]+', text)
    if number_match:
        number_str = number_match.group().replace(',', '')
        try:
            return int(number_str)
        except ValueError:
            return None
    return None


def parse_currency(text):
    # Parse money amounts
    if not text:
        return None
    
    # Remove currency symbols and prefixes
    text = re.sub(r'[US\$£€]', '', text)
    text = text.strip()
    
    # Handle "million" suffix
    if 'million' in text.lower():
        number_match = re.search(r'[\d.]+', text)
        if number_match:
            try:
                return float(number_match.group()) * 1_000_000
            except ValueError:
                return None
    
    # Handle regular numbers with commas
    number_match = re.search(r'[\d,]+\.?\d*', text)
    if number_match:
        number_str = number_match.group().replace(',', '')
        try:
            return float(number_str)
        except ValueError:
            return None
    
    return None


def scrape_event_page(url):
    # Scrape one event page
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        result = {
            'wikipedia_url': url,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Find the infobox
        infobox = soup.find('table', class_='infobox')
        if not infobox:
            # Try alternate infobox classes
            infobox = soup.find('table', class_='infobox vevent')
        
        if not infobox:
            print(f"No infobox found on {url}")
            return None
        
        # Extract title/event name
        title_element = soup.find('h1', id='firstHeading')
        if title_element:
            result['event_name'] = title_element.get_text(strip=True)
        
        # Parse infobox rows
        for row in infobox.find_all('tr'):
            header = row.find('th')
            data = row.find('td')
            
            if not header or not data:
                continue
            
            header_text = header.get_text(strip=True).lower()
            data_text = data.get_text(strip=True)
            
            # Attendance
            if 'attendance' in header_text:
                result['attendance'] = parse_number(data_text)
                result['attendance_raw'] = data_text
            
            # Gate revenue
            elif 'gate' in header_text:
                result['gate_revenue'] = parse_currency(data_text)
                result['gate_raw'] = data_text
            
            # Venue
            elif 'venue' in header_text:
                result['venue'] = data_text
                
                # Try to extract venue link for capacity lookup
                venue_link = data.find('a')
                if venue_link and venue_link.get('href'):
                    result['venue_wiki_path'] = venue_link.get('href')
            
            # Location/City
            elif 'city' in header_text or 'location' in header_text:
                result['location'] = data_text
            
            # Date
            elif 'date' in header_text:
                result['date_raw'] = data_text
                # Try to parse date
                try:
                    # Handle various date formats
                    for fmt in ['%B %d, %Y', '%d %B %Y', '%Y-%m-%d']:
                        try:
                            result['event_date'] = datetime.strptime(
                                data_text.split('[')[0].strip(), fmt
                            ).strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass
            
            # Buy rate
            elif 'buyrate' in header_text.replace(' ', '') or 'buys' in header_text:
                result['ppv_buys'] = parse_number(data_text)
                result['ppv_buys_raw'] = data_text
        
        return result if result.get('attendance') or result.get('gate_revenue') else None
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return None
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None


def get_ufc_event_links(limit=None):
    # Get list of UFC event URLs from Wikipedia
    print("Fetching UFC event list...")
    
    list_url = "https://en.wikipedia.org/wiki/List_of_UFC_events"
    
    try:
        response = requests.get(list_url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        events = []
        
        # Find all wikitables
        tables = soup.find_all('table', class_='wikitable')
        
        # Also try finding links in the main content area
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Filter for UFC event pages
            if (href.startswith('/wiki/UFC') and 
                'List' not in text and 
                'disambiguation' not in href.lower() and
                text and
                len(text) > 3):  # Filter out very short text
                full_url = f"https://en.wikipedia.org{href}"
                events.append((text, full_url))
        
        for table in tables:
            rows = table.find_all('tr')
            
            for row in rows[1:]:  # Skip header row
                cells = row.find_all(['td', 'th'])
                
                if len(cells) >= 1:
                    # First cell usually contains the event name/link
                    first_cell = cells[0]
                    link = first_cell.find('a')
                    
                    if link:
                        href = link.get('href', '')
                        text = link.get_text(strip=True)
                        
                        # Filter for UFC event pages
                        if href.startswith('/wiki/UFC') and 'List' not in text:
                            full_url = f"https://en.wikipedia.org{href}"
                            events.append((text, full_url))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_events = []
        for name, url in events:
            if url not in seen:
                seen.add(url)
                unique_events.append((name, url))
        
        print(f"Found {len(unique_events)} UFC event pages")
        
        if limit:
            unique_events = unique_events[:limit]
        
        return unique_events
        
    except Exception as e:
        print(f"Failed to fetch event list: {e}")
        return []


def scrape_venue_capacity(venue_wiki_path):
    # Get venue capacity from Wikipedia
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
                    # Capacity might be listed for different configurations
                    # Take the first/largest number
                    numbers = re.findall(r'[\d,]+', data_text)
                    if numbers:
                        capacities = [int(n.replace(',', '')) for n in numbers]
                        return max(capacities)
        
        return None
        
    except Exception as e:
        print(f"Failed to scrape venue capacity from {url}: {e}")
        return None


def scrape_all_events(output_path, limit=None, resume=True):
    # Scrape all events
    # Load existing data if resuming
    existing_data = []
    existing_urls = set()
    
    if resume and os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        existing_data = existing_df.to_dict('records')
        existing_urls = set(existing_df['wikipedia_url'].dropna())
        print(f"Loaded {len(existing_data)} existing records")
    
    # Get event list
    events = get_ufc_event_links(limit=limit)
    
    # Scrape each event
    all_data = existing_data.copy()
    new_count = 0
    
    for i, (event_name, url) in enumerate(events):
        if url in existing_urls:
            continue
        
        print(f"[{i+1}/{len(events)}] Scraping: {event_name}")
        
        data = scrape_event_page(url)
        
        if data:
            # Try to get venue capacity if we have a venue link
            if data.get('venue_wiki_path'):
                time.sleep(REQUEST_DELAY / 2)  # Smaller delay for venue lookup
                capacity = scrape_venue_capacity(data['venue_wiki_path'])
                if capacity:
                    data['venue_capacity'] = capacity
            
            all_data.append(data)
            new_count += 1
            
            # Save every 10
            if new_count % 10 == 0:
                df = pd.DataFrame(all_data)
                df.to_csv(output_path, index=False)
                print(f"Saved {len(all_data)} records")
        
        time.sleep(REQUEST_DELAY)
    
    # Final save
    df = pd.DataFrame(all_data)
    
    if not df.empty:
        # Calculate sell-through where possible
        df['sell_through'] = df.apply(
            lambda row: row['attendance'] / row['venue_capacity'] 
            if pd.notna(row.get('attendance')) and pd.notna(row.get('venue_capacity')) and row.get('venue_capacity', 0) > 0
            else None,
            axis=1
        )
        
        # Sort by date
        if 'event_date' in df.columns:
            df = df.sort_values('event_date', ascending=False)
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
    print(f"Done! Total: {len(df)} events, New: {new_count}")
    
    # Print summary
    if not df.empty:
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Total events scraped: {len(df)}")
        print(f"Events with attendance: {df['attendance'].notna().sum()}")
        print(f"Events with gate revenue: {df['gate_revenue'].notna().sum()}")
        print(f"Events with venue capacity: {df['venue_capacity'].notna().sum()}")
        print(f"Events with sell-through: {df['sell_through'].notna().sum()}")
        
        if df['attendance'].notna().any():
            print(f"\nAttendance range: {df['attendance'].min():,.0f} - {df['attendance'].max():,.0f}")
        
        if df['sell_through'].notna().any():
            print(f"Sell-through range: {df['sell_through'].min():.1%} - {df['sell_through'].max():.1%}")
        
        print("="*60)
    
    return df


def merge_with_ufcstats(attendance_df, events_path):
    # Merge with UFCStats data
    if not os.path.exists(events_path):
        print(f"UFCStats events file not found: {events_path}")
        return attendance_df
    
    events_df = pd.read_csv(events_path)
    
    # Normalize event names for matching
    def normalize_name(name):
        if pd.isna(name):
            return ""
        # Remove special characters and lowercase
        return re.sub(r'[^a-z0-9]', '', str(name).lower())
    
    attendance_df['event_name_normalized'] = attendance_df['event_name'].apply(normalize_name)
    events_df['event_name_normalized'] = events_df['event'].apply(normalize_name)
    
    # Merge on normalized name
    merged = events_df.merge(
        attendance_df[['event_name_normalized', 'attendance', 'gate_revenue', 
                       'venue_capacity', 'sell_through', 'ppv_buys']],
        on='event_name_normalized',
        how='left'
    )
    
    print(f"Merged {merged['attendance'].notna().sum()}/{len(merged)} events with attendance data")
    
    return merged


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape UFC attendance from Wikipedia")
    parser.add_argument("--output", default="./data/external/attendance.csv",
                        help="Output CSV path")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of events to scrape (for testing)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh instead of resuming")
    parser.add_argument("--merge-with", default=None,
                        help="Path to UFCStats events CSV to merge with")
    
    args = parser.parse_args()
    
    # Run scraping
    df = scrape_all_events(
        output_path=args.output,
        limit=args.limit,
        resume=not args.no_resume
    )
    
    # Optionally merge with UFCStats data
    if args.merge_with and os.path.exists(args.merge_with):
        merged = merge_with_ufcstats(df, args.merge_with)
        merged_path = args.output.replace('.csv', '_merged.csv')
        merged.to_csv(merged_path, index=False)
        print(f"\nMerged data saved to: {merged_path}")
    
    print(f"\nAttendance data saved to: {args.output}")
