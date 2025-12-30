# Scrape Reddit sentiment
# DS 410 project

import os
import time
import random
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse

# Try to import PRAW
try:
    import praw
    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False
    print("praw not installed")

# Try to import textblob
try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False
    print("textblob not installed")

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = "UFC_SellThrough_DS410/1.0"


def create_reddit_client():
    # Create Reddit client
    if not PRAW_AVAILABLE:
        return None
    
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        print("Reddit API credentials not set")
        return None
    
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        return reddit
    except Exception as e:
        print(f"Failed to create Reddit client: {e}")
        return None


def analyze_sentiment(text):
    # Analyze sentiment with TextBlob
    if not TEXTBLOB_AVAILABLE or not text:
        return {"polarity": 0.0, "subjectivity": 0.5}
    
    try:
        blob = TextBlob(str(text))
        return {
            "polarity": blob.sentiment.polarity,
            "subjectivity": blob.sentiment.subjectivity
        }
    except Exception:
        return {"polarity": 0.0, "subjectivity": 0.5}


def simple_sentiment(text):
    # Simple sentiment without libraries
    if not text:
        return {"polarity": 0.0, "subjectivity": 0.5}
    
    text = text.lower()
    
    # Positive words common in MMA discussions
    positive_words = [
        "hype", "excited", "hyped", "amazing", "great", "awesome", "sick",
        "fire", "banger", "war", "lets go", "pumped", "stacked", "main event",
        "championship", "knockout", "ko", "finish", "dominant", "beast", "goat",
        "legend", "best", "favorite", "love", "can't wait", "crazy", "insane"
    ]
    
    # Negative words
    negative_words = [
        "boring", "trash", "weak", "bad", "sucks", "terrible", "worst",
        "skip", "pass", "meh", "disappointed", "disappointing", "robbery",
        "overrated", "fraud", "ducking", "scared", "padded", "can", "joke"
    ]
    
    # Count matches
    pos_count = sum(1 for word in positive_words if word in text)
    neg_count = sum(1 for word in negative_words if word in text)
    
    total = pos_count + neg_count
    if total == 0:
        polarity = 0.0
    else:
        polarity = (pos_count - neg_count) / total
    
    # Subjectivity based on emotional word density
    words = text.split()
    word_count = len(words) if words else 1
    subjectivity = min(1.0, (pos_count + neg_count) / (word_count / 10))
    
    return {"polarity": polarity, "subjectivity": subjectivity}


def scrape_event_thread(reddit, event_name, subreddit="MMA"):
    # Scrape comments from event thread
    if reddit is None:
        return []
    
    comments_data = []
    
    try:
        # Search for event discussion thread
        subreddit_obj = reddit.subreddit(subreddit)
        search_query = f"{event_name} discussion"
        
        for submission in subreddit_obj.search(search_query, limit=5):
            # Check if this is a relevant discussion thread
            if event_name.lower() in submission.title.lower():
                submission.comments.replace_more(limit=0)
                
                for comment in submission.comments.list()[:100]:  # Limit per thread
                    if hasattr(comment, "body"):
                        sentiment = analyze_sentiment(comment.body) if TEXTBLOB_AVAILABLE else simple_sentiment(comment.body)
                        
                        comments_data.append({
                            "event_name": event_name,
                            "thread_title": submission.title,
                            "comment_text": comment.body[:500],  # Truncate long comments
                            "score": comment.score,
                            "created_utc": datetime.fromtimestamp(comment.created_utc),
                            "polarity": sentiment["polarity"],
                            "subjectivity": sentiment["subjectivity"]
                        })
                
                break  # Found the main thread
        
    except Exception as e:
        pass
    
    return comments_data


def scrape_mma_subreddit(event_names, delay_seconds=2.0):
    # Scrape r/MMA for events
    print(f"Scraping r/MMA for {len(event_names)} events...")
    
    reddit = create_reddit_client()
    if reddit is None:
        print("Cannot connect to Reddit API")
        return pd.DataFrame()
    
    all_comments = []
    
    for i, event_name in enumerate(event_names):
        comments = scrape_event_thread(reddit, event_name)
        all_comments.extend(comments)
        
        if (i + 1) % 10 == 0:
            print(f"  Scraped {i + 1}/{len(event_names)} events")
        
        time.sleep(delay_seconds)
    
    df = pd.DataFrame(all_comments)
    print(f"Scraped {len(df)} total comments")
    
    return df


def generate_synthetic_comments(events_df, comments_per_event=500):
    # Generate fake comments for training
    print(f"Generating synthetic comments for {len(events_df)} events...")
    
    # Sample comment templates with varying sentiment
    positive_templates = [
        "This card is absolutely STACKED! Can't wait!",
        "Main event is going to be insane, both fighters are in their prime",
        "Finally a good card, this is going to be a banger",
        "SO HYPED for this fight! Been waiting for this matchup",
        "This is the fight to make, great booking by the UFC",
        "Early FOTY candidate for sure",
        "Both guys are killers, expecting fireworks",
        "War {fighter}! Let's go!",
        "This is why I love MMA",
        "Going to be watching with the boiiiis",
    ]
    
    neutral_templates = [
        "Interesting matchup, could go either way",
        "Not sure about this one, we'll see",
        "The co-main is solid at least",
        "I'll watch it but not super excited",
        "Decent card overall",
        "Some good fights on here",
        "Main event should be competitive",
        "Not bad, not great",
    ]
    
    negative_templates = [
        "This card is weak, might skip it",
        "Why is this even a main event?",
        "Boring matchup, nobody asked for this",
        "Another fight night that feels like a PPV filler",
        "The prelims look better than the main card",
        "Dana is really pushing some weird matchups lately",
        "This is mid at best",
        "Probably going to be a wrestle-fest, passing",
    ]
    
    all_comments = []
    
    for _, event in events_df.iterrows():
        event_name = event.get("event", event.get("event_name", "UFC Event"))
        
        # Determine event quality affects sentiment distribution
        is_ppv = "UFC " in str(event_name) and "Fight Night" not in str(event_name)
        
        # PPV events get more positive sentiment
        if is_ppv:
            pos_weight, neu_weight, neg_weight = 0.5, 0.35, 0.15
        else:
            pos_weight, neu_weight, neg_weight = 0.35, 0.4, 0.25
        
        # Generate comments
        for _ in range(comments_per_event):
            # Choose sentiment category
            rand = random.random()
            if rand < pos_weight:
                template = random.choice(positive_templates)
                base_polarity = random.uniform(0.3, 0.9)
            elif rand < pos_weight + neu_weight:
                template = random.choice(neutral_templates)
                base_polarity = random.uniform(-0.2, 0.2)
            else:
                template = random.choice(negative_templates)
                base_polarity = random.uniform(-0.9, -0.3)
            
            # Add some variation
            polarity = base_polarity + random.gauss(0, 0.1)
            polarity = max(-1, min(1, polarity))
            
            subjectivity = random.uniform(0.4, 0.9)
            
            # Generate fake timestamp
            event_date = event.get("date", event.get("event_date"))
            if pd.notna(event_date):
                try:
                    base_date = pd.to_datetime(event_date)
                    # Comments come 1-7 days before event
                    comment_date = base_date - timedelta(days=random.randint(1, 7))
                except:
                    comment_date = datetime.now() - timedelta(days=random.randint(1, 365))
            else:
                comment_date = datetime.now() - timedelta(days=random.randint(1, 365))
            
            # Generate fake score upvotes
            base_score = 10 if is_ppv else 5
            score = max(1, int(random.expovariate(1/base_score)))
            
            all_comments.append({
                "event_name": event_name,
                "thread_title": f"[Official] {event_name} - Discussion Thread",
                "comment_text": template,
                "score": score,
                "created_utc": comment_date,
                "polarity": polarity,
                "subjectivity": subjectivity,
                "source": "Synthetic"
            })
    
    df = pd.DataFrame(all_comments)
    print(f"Generated {len(df)} synthetic comments")
    
    return df


def aggregate_event_sentiment(comments_df):
    # Aggregate sentiment to event level
    print("Aggregating sentiment to event level...")
    
    if comments_df.empty:
        return pd.DataFrame()
    
    # Group by event
    event_sentiment = comments_df.groupby("event_name").agg({
        "polarity": ["mean", "std", "min", "max"],
        "subjectivity": ["mean"],
        "score": ["sum", "mean", "max"],
        "comment_text": "count"
    }).reset_index()
    
    # Flatten column names
    event_sentiment.columns = [
        "event_name",
        "avg_sentiment", "sentiment_std", "min_sentiment", "max_sentiment",
        "avg_subjectivity",
        "total_engagement", "avg_comment_score", "top_comment_score",
        "comment_count"
    ]
    
    # Calculate derived features
    event_sentiment["sentiment_range"] = event_sentiment["max_sentiment"] - event_sentiment["min_sentiment"]
    event_sentiment["hype_score"] = (
        (event_sentiment["avg_sentiment"] + 1) / 2 *  # Normalize to 0-1
        np.log1p(event_sentiment["total_engagement"]) /  # Engagement factor
        10  # Scale down
    )
    
    # Classify sentiment
    event_sentiment["sentiment_category"] = pd.cut(
        event_sentiment["avg_sentiment"],
        bins=[-1, -0.2, 0.2, 1],
        labels=["Negative", "Neutral", "Positive"]
    )
    
    print(f"Aggregated sentiment for {len(event_sentiment)} events")
    
    return event_sentiment


def run_reddit_pipeline(data_dir, output_dir, use_synthetic=True, max_events=200, comments_per_event=500):
    # Main Reddit sentiment scraper
    print("=" * 60)
    print("UFC Reddit Sentiment Pipeline")
    print("=" * 60)
    
    # Output paths
    comments_path = os.path.join(output_dir, "external", "reddit_comments.csv")
    sentiment_path = os.path.join(output_dir, "external", "event_sentiment.csv")
    os.makedirs(os.path.dirname(comments_path), exist_ok=True)
    
    # Load events
    events_path = os.path.join(data_dir, "raw", "events.csv")
    if not os.path.exists(events_path):
        print(f"Events file not found at {events_path}")
        return
    
    events_df = pd.read_csv(events_path)
    events_df.columns = [c.lower() for c in events_df.columns]
    
    print(f"Loaded {len(events_df)} events")
    
    # Check for existing data
    comments_df = None
    if os.path.exists(comments_path):
        print(f"Loading existing comments from {comments_path}")
        comments_df = pd.read_csv(comments_path)
        print(f"Loaded {len(comments_df)} existing comments")
    
    # Scrape or generate comments
    if comments_df is None or len(comments_df) == 0:
        if use_synthetic or not PRAW_AVAILABLE:
            # Generate synthetic data
            events_subset = events_df.head(max_events)
            comments_df = generate_synthetic_comments(events_subset, comments_per_event)
        else:
            # Scrape from Reddit
            event_names = events_df["event"].head(max_events).tolist()
            comments_df = scrape_mma_subreddit(event_names)
    
    if comments_df.empty:
        print("Failed to obtain Reddit comment data!")
        return
    
    # Save raw comments
    comments_df.to_csv(comments_path, index=False)
    print(f"Saved {len(comments_df)} comments to {comments_path}")
    
    # Aggregate to event level
    event_sentiment = aggregate_event_sentiment(comments_df)
    
    if not event_sentiment.empty:
        event_sentiment.to_csv(sentiment_path, index=False)
        print(f"Saved sentiment for {len(event_sentiment)} events to {sentiment_path}")
    
    # Print summary
    print("\nReddit Sentiment Summary:")
    print(f"  Total comments: {len(comments_df)}")
    print(f"  Events with data: {comments_df['event_name'].nunique()}")
    print(f"  Avg sentiment: {comments_df['polarity'].mean():.3f}")
    if not event_sentiment.empty:
        print(f"  Positive events: {(event_sentiment['sentiment_category'] == 'Positive').sum()}")
        print(f"  Neutral events: {(event_sentiment['sentiment_category'] == 'Neutral').sum()}")
        print(f"  Negative events: {(event_sentiment['sentiment_category'] == 'Negative').sum()}")
        print(f"  Avg hype score: {event_sentiment['hype_score'].mean():.3f}")
    
    print("\n" + "=" * 60)
    print("Reddit Sentiment Pipeline Complete!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UFC Reddit Sentiment Scraper")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--output-dir", default="./data", help="Output directory")
    parser.add_argument("--real-api", action="store_true", help="Use real Reddit API")
    parser.add_argument("--max-events", type=int, default=200, help="Max events to process")
    parser.add_argument("--comments-per-event", type=int, default=500, help="Comments per event (synthetic)")
    
    args = parser.parse_args()
    
    run_reddit_pipeline(
        args.data_dir,
        args.output_dir,
        use_synthetic=not args.real_api,
        max_events=args.max_events,
        comments_per_event=args.comments_per_event
    )

