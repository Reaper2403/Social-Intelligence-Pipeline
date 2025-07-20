import praw
import csv
import json
import os
import re
from dotenv import load_dotenv
from praw.exceptions import PRAWException
from datetime import datetime, timezone
import time
import hashlib
from pathlib import Path

# --- MODIFIED: Define the data directory relative to the project root ---
DATA_DIR = "data"

# Opportunity Score Constants and Formulas
W1_SCORE_VELOCITY = 1.0
W2_COMMENT_VELOCITY = 1.5
W3_COMMENT_SCORE = 1.0
W4_COMMENT_REPLIES = 2.0
AGE_SMOOTHING_FACTOR = 2

# Load Reddit API credentials from .env file
load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

class MasterDataStore:
    """Manages a persistent store of all previously extracted posts and comments"""
    
    def __init__(self, master_file="master_reddit_data.json"):
        self.master_file = master_file # <-- This path will be constructed with DATA_DIR
        self.post_ids = set()
        self.comment_ids = set()
        self.posts_data = {}
        self.load_master_data()
    
    def load_master_data(self):
        """Load existing master data if available"""
        if Path(self.master_file).exists():
            try:
                with open(self.master_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                if 'posts' in data:
                    for post in data['posts']:
                        post_id = post['post_details']['id'] if 'id' in post['post_details'] else post['post_details']['permalink'].split('/')[-2]
                        self.post_ids.add(post_id)
                        self.posts_data[post_id] = post
                        
                        # Load comment IDs
                        for comment in post.get('top_comments', []):
                            if 'id' in comment:
                                self.comment_ids.add(comment['id'])
                
                print(f"✅ Loaded {len(self.post_ids)} existing posts from master data store")
            except Exception as e:
                print(f"⚠️  Error loading master data: {e}")
                print("   Starting with empty master data store")
    
    def is_post_processed(self, post_id):
        """Check if a post has already been processed"""
        return post_id in self.post_ids
    
    def is_comment_processed(self, comment_id):
        """Check if a comment has already been processed"""
        return comment_id in self.comment_ids
    
    def add_post(self, post_data):
        """Add a new post to the master store"""
        post_id = post_data['post_details']['id']
        self.post_ids.add(post_id)
        self.posts_data[post_id] = post_data
        
        # Add comment IDs
        for comment in post_data.get('top_comments', []):
            if 'id' in comment:
                self.comment_ids.add(comment['id'])
    
    def update_post_keywords(self, post_id, new_keywords):
        """Update the keywords list for an existing post"""
        if post_id in self.posts_data:
            existing_keywords = set(self.posts_data[post_id]['search_info']['keywords'])
            updated_keywords = existing_keywords.union(set(new_keywords))
            self.posts_data[post_id]['search_info']['keywords'] = list(updated_keywords)
            print(f"   ↻ Updated keywords for existing post: {updated_keywords}")
    
    def save_master_data(self):
        """Save master data to file"""
        try:
            export_data = {
                "master_data_info": {
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "total_posts": len(self.posts_data),
                    "script_version": "2.1",
                    "opportunity_scoring": {
                        "W1_SCORE_VELOCITY": W1_SCORE_VELOCITY,
                        "W2_COMMENT_VELOCITY": W2_COMMENT_VELOCITY,
                        "W3_COMMENT_SCORE": W3_COMMENT_SCORE,
                        "W4_COMMENT_REPLIES": W4_COMMENT_REPLIES,
                        "AGE_SMOOTHING_FACTOR": AGE_SMOOTHING_FACTOR
                    }
                },
                "posts": list(self.posts_data.values())
            }
            
            with open(self.master_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Master data saved to {self.master_file}")
        except Exception as e:
            print(f"❌ Error saving master data: {e}")
    
    def get_new_posts_only(self):
        """Return only the posts that were added in this session"""
        return [post for post in self.posts_data.values() 
                if post.get('newly_added', False)]

def generate_post_id(submission):
    """Generate a unique ID for a post based on its permalink"""
    return submission.id if hasattr(submission, 'id') else submission.permalink.split('/')[-2]

def generate_comment_id(comment):
    """Generate a unique ID for a comment"""
    return comment.id if hasattr(comment, 'id') else str(hash(comment.body + str(comment.created_utc)))

def calculate_post_opportunity_score(score, upvote_ratio, num_comments, age_hours):
    """
    Calculate opportunity score for a post using the formula:
    OS_post = (W1 * (score * upvote_ratio) / (age_hours + S_FACTOR)) + (W2 * num_comments / (age_hours + S_FACTOR))
    """
    if age_hours + AGE_SMOOTHING_FACTOR == 0:
        return 0
    
    score_velocity = W1_SCORE_VELOCITY * (score * upvote_ratio) / (age_hours + AGE_SMOOTHING_FACTOR)
    comment_velocity = W2_COMMENT_VELOCITY * num_comments / (age_hours + AGE_SMOOTHING_FACTOR)
    
    return round(score_velocity + comment_velocity, 2)

def calculate_reply_opportunity_score(post_opportunity_score, comment_score, num_replies, depth):
    """
    Calculate opportunity score for a reply using the formula:
    OS_reply = OS_post + (W3 * score_comment + W4 * num_replies) * (1 / (depth + 1))
    """
    depth_factor = 1 / (depth + 1)
    comment_component = (W3_COMMENT_SCORE * comment_score + W4_COMMENT_REPLIES * num_replies) * depth_factor
    
    return round(post_opportunity_score + comment_component, 2)

def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✅ Configuration loaded from {config_file}")
        return config
    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found!")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON in '{config_file}': {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def validate_config(config):
    """Validate configuration parameters"""
    if not config:
        return False
    
    # Check required sections
    required_sections = ['search_settings', 'export_settings', 'api_settings', 'filter_settings']
    for section in required_sections:
        if section not in config:
            print(f"❌ Missing required configuration section: {section}")
            return False
    
    return True

def print_config_summary(config):
    """Print a summary of the current configuration"""
    search = config['search_settings']
    export = config['export_settings']
    
    print("📋 CURRENT CONFIGURATION:")
    print(f"   Keywords: {', '.join(search['keywords'])}")
    
    if search.get('search_all_reddit', False):
        print("   Target: All of Reddit")
    else:
        subreddits = search.get('target_subreddits', [])
        if subreddits:
            print(f"   Subreddits: {', '.join(subreddits)}")
        else:
            print("   Target: All of Reddit (no specific subreddits)")
    
    print(f"   Posts per keyword: {search['posts_per_keyword']}")
    print(f"   Sort method: {search['sort_method']}")
    print(f"   Export format: {export['format'].upper()}")
    print("-" * 50)

def clean_text(text):
    """Clean text by removing extra whitespace and newlines for CSV"""
    if text is None:
        return ""
    # Remove extra whitespace and replace newlines with spaces
    cleaned = re.sub(r'\s+', ' ', text.strip())
    # Remove or escape characters that might break CSV
    cleaned = cleaned.replace('"', '""')  # Escape quotes for CSV
    return cleaned

def extract_images_from_post(submission):
    """Extract image URLs from Reddit post"""
    images = []
    
    # Direct image URL
    if hasattr(submission, 'url') and submission.url:
        if any(ext in submission.url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
            images.append(submission.url)
    
    # Reddit gallery
    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        if hasattr(submission, 'media_metadata') and submission.media_metadata:
            for item in submission.media_metadata.values():
                if 's' in item and 'u' in item['s']:
                    # Convert preview URL to direct image URL
                    img_url = item['s']['u'].replace('preview.redd.it', 'i.redd.it')
                    images.append(img_url)
    
    return '; '.join(images) if images else ""

def search_reddit_posts_deduplicated(config, master_store):
    """
    Search Reddit for posts with deduplication and keyword aggregation
    """
    search_settings = config['search_settings']
    api_settings = config['api_settings']
    filter_settings = config['filter_settings']
    
    keywords = search_settings['keywords']
    subreddits = search_settings.get('target_subreddits', [])
    limit_per_keyword = search_settings['posts_per_keyword']
    sort_method = search_settings['sort_method']
    time_filter = search_settings['time_filter']
    
    # Track posts found in this session with their matching keywords
    session_posts = {}  # post_id -> {submission, matching_keywords}
    new_posts_count = 0
    updated_posts_count = 0
    
    try:
        # Search for each keyword across all subreddits
        for keyword in keywords:
            print(f"\n🔍 Searching for keyword: '{keyword}'...")
            
            for subreddit_name in subreddits:
                try:
                    subreddit = reddit.subreddit(subreddit_name)
                    search_query = f"{keyword}"
                    
                    if sort_method.lower() == "top":
                        posts = subreddit.search(search_query, sort="top", time_filter=time_filter, limit=limit_per_keyword)
                    elif sort_method.lower() == "hot":
                        posts = subreddit.search(search_query, sort="hot", limit=limit_per_keyword)
                    elif sort_method.lower() == "new":
                        posts = subreddit.search(search_query, sort="new", limit=limit_per_keyword)
                    elif sort_method.lower() == "controversial":
                        posts = subreddit.search(search_query, sort="controversial", time_filter=time_filter, limit=limit_per_keyword)
                    
                    # Process posts and group by ID
                    for submission in posts:
                        try:
                            # Apply basic filters
                            if filter_settings.get('exclude_nsfw', False) and submission.over_18:
                                continue
                            if submission.score < filter_settings.get('min_score', 0):
                                continue
                            if filter_settings.get('exclude_deleted_posts', True) and (
                                submission.author is None or submission.selftext == '[deleted]'
                            ):
                                continue
                            
                            post_id = generate_post_id(submission)
                            
                            # Check if we've seen this post in this session
                            if post_id in session_posts:
                                # Add this keyword to the existing post
                                session_posts[post_id]['matching_keywords'].add(keyword)
                                print(f"   ↻ Found duplicate post, added keyword '{keyword}'")
                            else:
                                # New post in this session
                                session_posts[post_id] = {
                                    'submission': submission,
                                    'matching_keywords': {keyword},
                                    'subreddit_searched': subreddit_name
                                }
                            
                        except Exception as e:
                            print(f"   ⚠️  Error processing post in r/{subreddit_name}: {e}")
                            continue
                    
                    time.sleep(api_settings.get('rate_limit_delay', 1.0))
                    
                except Exception as e:
                    print(f"   ❌ Error searching in r/{subreddit_name}: {e}")
                    continue
        
        print(f"\n📊 Found {len(session_posts)} unique posts across all keywords")
        
        # Process each unique post
        for post_id, post_info in session_posts.items():
            submission = post_info['submission']
            matching_keywords = list(post_info['matching_keywords'])
            
            # Check if this post already exists in master store
            if master_store.is_post_processed(post_id):
                print(f"   ↻ Post already in master store, updating keywords...")
                master_store.update_post_keywords(post_id, matching_keywords)
                updated_posts_count += 1
                continue
            
            # Process new post
            print(f"   ✨ Processing new post: {submission.title[:50]}...")
            
            try:
                # Calculate post metrics
                now_utc = datetime.now(timezone.utc)
                post_created_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
                age_hours = (now_utc - post_created_time).total_seconds() / 3600
                age_hours = round(age_hours, 2)
                
                post_opportunity_score = calculate_post_opportunity_score(
                    submission.score, 
                    submission.upvote_ratio, 
                    submission.num_comments, 
                    age_hours
                )
                
                # Process comments
                comments_data = []
                if config['export_settings'].get('include_comments', True):
                    submission.comments.replace_more(limit=0)
                    max_comments = config['export_settings'].get('max_comments_per_post', 10)
                    top_comments = sorted(submission.comments, key=lambda x: x.score, reverse=True)[:max_comments]
                    
                    for i, comment in enumerate(top_comments, 1):
                        try:
                            if filter_settings.get('exclude_deleted_comments', True) and (
                                comment.author is None or comment.body == '[deleted]'
                            ):
                                continue
                            
                            comment_id = generate_comment_id(comment)
                            
                            # Skip if comment already processed
                            if master_store.is_comment_processed(comment_id):
                                continue
                            
                            num_replies = len(comment.replies.list())
                            depth = getattr(comment, 'depth', 0)
                            
                            reply_opportunity_score = calculate_reply_opportunity_score(
                                post_opportunity_score,
                                comment.score,
                                num_replies,
                                depth
                            )
                            
                            comment_data = {
                                'id': comment_id,
                                'rank': i,
                                'author': str(comment.author) if comment.author else '[deleted]',
                                'body': clean_text(comment.body),
                                'score': comment.score,
                                'created_utc': datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                                'num_replies': num_replies,
                                'depth': depth,
                                'opportunity_score_reply': reply_opportunity_score
                            }
                            comments_data.append(comment_data)
                            
                        except Exception as e:
                            print(f"      ⚠️  Error processing comment: {e}")
                            continue
                
                # Create post data structure
                post_data = {
                    'search_info': {
                        'keywords': matching_keywords,
                        'subreddit_searched': post_info['subreddit_searched']
                    },
                    'post_details': {
                        'id': post_id,
                        'subreddit': submission.subreddit.display_name,
                        'title': clean_text(submission.title),
                        'body': clean_text(submission.selftext),
                        'author': str(submission.author) if submission.author else '[deleted]',
                        'score': submission.score,
                        'upvote_ratio': submission.upvote_ratio,
                        'num_comments': submission.num_comments,
                        'created_utc': datetime.fromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                        'url': submission.url,
                        'permalink': f"https://reddit.com{submission.permalink}",
                        'images': extract_images_from_post(submission) if config['export_settings'].get('include_images', True) else "",
                        'is_video': submission.is_video,
                        'over_18': submission.over_18,
                        'age_hours': age_hours,
                        'opportunity_score_post': post_opportunity_score
                    },
                    'top_comments': comments_data,
                    'newly_added': True  # Flag for this session
                }
                
                # Add to master store
                master_store.add_post(post_data)
                new_posts_count += 1
                
                time.sleep(api_settings.get('post_processing_delay', 0.5))
                
            except Exception as e:
                print(f"      ❌ Error processing post {post_id}: {e}")
                continue
    
    except Exception as e:
        print(f"❌ Unexpected error during search: {e}")
    
    print(f"\n✅ Search completed:")
    print(f"   📄 New posts added: {new_posts_count}")
    print(f"   🔄 Existing posts updated: {updated_posts_count}")
    
    return new_posts_count > 0 or updated_posts_count > 0

def export_session_data(master_store, config):
    """Export only the new posts from this session"""
    new_posts = [post for post in master_store.posts_data.values() 
                 if post.get('newly_added', False)]
    
    if not new_posts:
        print("ℹ️  No new posts to export from this session.")
        return
    
    export_settings = config['export_settings']
    filename = export_settings.get('custom_filename')
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reddit_session_{timestamp}.json"
    
    try:
        export_data = {
            "session_info": {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_new_posts": len(new_posts),
                "script_version": "2.1_deduplicated",
                "opportunity_scoring": {
                    "W1_SCORE_VELOCITY": W1_SCORE_VELOCITY,
                    "W2_COMMENT_VELOCITY": W2_COMMENT_VELOCITY,
                    "W3_COMMENT_SCORE": W3_COMMENT_SCORE,
                    "W4_COMMENT_REPLIES": W4_COMMENT_REPLIES,
                    "AGE_SMOOTHING_FACTOR": AGE_SMOOTHING_FACTOR
                }
            },
            "posts": new_posts
        }
        
        # --- MODIFIED: Ensure the file is saved in the data directory ---
        output_path = os.path.join(DATA_DIR, filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Session data exported to {output_path}")
        print(f"   📄 New posts: {len(new_posts)}")
        
    except Exception as e:
        print(f"❌ Error exporting session data: {e}")

def main():
    """Main function to run the enhanced Reddit scraper"""
    
    print("🚀 Reddit Scraper with Deduplication & Master Data Store v2.1")
    print(f"📊 Scoring Configuration:")
    print(f"   W1 (Score Velocity): {W1_SCORE_VELOCITY}")
    print(f"   W2 (Comment Velocity): {W2_COMMENT_VELOCITY}")
    print(f"   W3 (Comment Score): {W3_COMMENT_SCORE}")
    print(f"   W4 (Comment Replies): {W4_COMMENT_REPLIES}")
    print(f"   Age Smoothing Factor: {AGE_SMOOTHING_FACTOR}")
    print("-" * 50)
    
    # --- MODIFIED: Construct file paths using DATA_DIR ---
    config_path = os.path.join(DATA_DIR, "config.json")
    master_file_path = os.path.join(DATA_DIR, "master_reddit_data.json")
    
    # Load configuration
    config = load_config(config_path) # <-- MODIFIED
    if not config:
        print("❌ Failed to load configuration. Exiting.")
        return
    
    if not validate_config(config):
        print("❌ Configuration validation failed. Please check your config.json file.")
        return
    
    print_config_summary(config)
    
    # Initialize master data store
    master_store = MasterDataStore(master_file_path) # <-- MODIFIED
    
    # Search for posts with deduplication
    has_changes = search_reddit_posts_deduplicated(config, master_store)
    
    if has_changes:
        # Save updated master data
        master_store.save_master_data()
        
        # Export session data (new posts only)
        export_session_data(master_store, config)
        
        # Print statistics
        total_posts = len(master_store.posts_data)
        new_posts = len([p for p in master_store.posts_data.values() if p.get('newly_added', False)])
        
        print(f"\n🎉 Scraping completed!")
        print(f"   📊 Total posts in master store: {total_posts}")
        print(f"   ✨ New posts this session: {new_posts}")
        
        # Print opportunity score statistics for new posts
        if new_posts > 0:
            new_post_scores = [post['post_details']['opportunity_score_post'] 
                             for post in master_store.posts_data.values() 
                             if post.get('newly_added', False)]
            if new_post_scores:
                avg_score = sum(new_post_scores) / len(new_post_scores)
                max_score = max(new_post_scores)
                print(f"   📈 New Posts Opportunity Scores:")
                print(f"      Average: {avg_score:.2f}")
                print(f"      Highest: {max_score:.2f}")
    else:
        print("ℹ️  No new posts found or changes made.")

if __name__ == "__main__":
    main()