import praw
import csv
import json
import os
import re
from dotenv import load_dotenv
from praw.exceptions import PRAWException
from datetime import datetime
import time

# Load Reddit API credentials from .env file
load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✅ Configuration loaded from {config_file}")
        return config
    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found!")
        print("Creating a default configuration file...")
        create_default_config(config_file)
        return load_config(config_file)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON in '{config_file}': {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def create_default_config(config_file="config.json"):
    """Create a default configuration file"""
    default_config = {
        "search_settings": {
            "keywords": ["python", "datascience", "machine learning", "marketing"],
            "target_subreddits": ["learnpython", "datascience", "MachineLearning", "marketing", "programming"],
            "search_all_reddit": False,
            "posts_per_keyword": 5,
            "sort_method": "top",
            "time_filter": "week"
        },
        "export_settings": {
            "format": "csv",
            "custom_filename": None,
            "include_images": True,
            "include_comments": True,
            "max_comments_per_post": 10
        },
        "api_settings": {
            "rate_limit_delay": 1.0,
            "post_processing_delay": 0.5,
            "max_retries": 3
        },
        "filter_settings": {
            "min_score": 0,
            "exclude_nsfw": False,
            "exclude_deleted_posts": True,
            "exclude_deleted_comments": True
        }
    }
    
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2)
        print(f"✅ Default configuration created: {config_file}")
    except Exception as e:
        print(f"❌ Error creating default configuration: {e}")

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
    
    # Validate sort method
    valid_sort_methods = ['top', 'hot', 'new', 'controversial']
    sort_method = config['search_settings'].get('sort_method', '').lower()
    if sort_method not in valid_sort_methods:
        print(f"❌ Invalid sort_method: {sort_method}. Must be one of: {valid_sort_methods}")
        return False
    
    # Validate time filter
    valid_time_filters = ['hour', 'day', 'week', 'month', 'year', 'all']
    time_filter = config['search_settings'].get('time_filter', '').lower()
    if time_filter not in valid_time_filters:
        print(f"❌ Invalid time_filter: {time_filter}. Must be one of: {valid_time_filters}")
        return False
    
    # Validate export format
    valid_formats = ['csv', 'json']
    export_format = config['export_settings'].get('format', '').lower()
    if export_format not in valid_formats:
        print(f"❌ Invalid export format: {export_format}. Must be one of: {valid_formats}")
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
    
    if search['sort_method'] in ['top', 'controversial']:
        print(f"   Time filter: {search['time_filter']}")
    
    print(f"   Export format: {export['format'].upper()}")
    
    if export['custom_filename']:
        print(f"   Filename: {export['custom_filename']}")
    else:
        print("   Filename: Auto-generated with timestamp")
    
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

def search_reddit_posts(config):
    """
    Search Reddit for posts containing keywords using configuration
    """
    search_settings = config['search_settings']
    api_settings = config['api_settings']
    filter_settings = config['filter_settings']
    
    keywords = search_settings['keywords']
    subreddits = None if search_settings.get('search_all_reddit', False) else search_settings.get('target_subreddits')
    limit = search_settings['posts_per_keyword']
    sort_method = search_settings['sort_method']
    time_filter = search_settings['time_filter']
    
    all_posts_data = []
    
    try:
        for keyword in keywords:
            print(f"Searching for keyword: '{keyword}'...")
            
            # Search across specified subreddits or all of Reddit
            if subreddits:
                search_query = f"{keyword}"
                for subreddit_name in subreddits:
                    try:
                        subreddit = reddit.subreddit(subreddit_name)
                        posts = get_posts_by_method(subreddit, search_query, sort_method, time_filter, limit)
                        process_posts(posts, keyword, subreddit_name, all_posts_data, limit, config)
                    except Exception as e:
                        print(f"Error searching in r/{subreddit_name}: {e}")
                        if api_settings.get('max_retries', 0) > 0:
                            print(f"Retrying in {api_settings['rate_limit_delay']} seconds...")
                            time.sleep(api_settings['rate_limit_delay'])
            else:
                # Search all of Reddit
                try:
                    all_subreddit = reddit.subreddit("all")
                    posts = get_posts_by_method(all_subreddit, keyword, sort_method, time_filter, limit)
                    process_posts(posts, keyword, "all", all_posts_data, limit, config)
                except Exception as e:
                    print(f"Error searching all of Reddit for '{keyword}': {e}")
            
            # Add delay to respect rate limits
            time.sleep(api_settings.get('rate_limit_delay', 1.0))
                    
    except PRAWException as e:
        print(f"Reddit API error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    return all_posts_data

def get_posts_by_method(subreddit, search_query, sort_method, time_filter, limit):
    """
    Get posts based on the specified sorting method
    
    Args:
        subreddit: PRAW subreddit object
        search_query: Search query string
        sort_method: Sort method (top, hot, new, controversial)
        time_filter: Time filter for top/controversial (ignored for hot/new)
        limit: Number of posts to fetch
    """
    sort_method = sort_method.lower()
    
    if sort_method == "top":
        return subreddit.search(search_query, sort="top", time_filter=time_filter, limit=limit)
    elif sort_method == "hot":
        return subreddit.search(search_query, sort="hot", limit=limit)
    elif sort_method == "new":
        return subreddit.search(search_query, sort="new", limit=limit)
    elif sort_method == "controversial":
        return subreddit.search(search_query, sort="controversial", time_filter=time_filter, limit=limit)
    else:
        print(f"Warning: Unknown sort method '{sort_method}', defaulting to 'top'")
        return subreddit.search(search_query, sort="top", time_filter=time_filter, limit=limit)

def process_posts(posts, keyword, subreddit_name, all_posts_data, limit, config):
    """Process posts and extract data with configuration-based filtering"""
    post_count = 0
    filter_settings = config['filter_settings']
    export_settings = config['export_settings']
    api_settings = config['api_settings']
    
    for submission in posts:
        if post_count >= limit:
            break
            
        try:
            # Apply filters
            if filter_settings.get('exclude_nsfw', False) and submission.over_18:
                continue
                
            if submission.score < filter_settings.get('min_score', 0):
                continue
                
            if filter_settings.get('exclude_deleted_posts', True) and (
                submission.author is None or submission.selftext == '[deleted]'
            ):
                continue
            
            print(f"Processing post: {submission.title[:50]}...")
            
            # Get top comments based on configuration
            max_comments = export_settings.get('max_comments_per_post', 10)
            if export_settings.get('include_comments', True):
                submission.comments.replace_more(limit=0)
                top_comments = sorted(submission.comments, key=lambda x: x.score, reverse=True)[:max_comments]
                
                comments_data = []
                for i, comment in enumerate(top_comments, 1):
                    # Filter deleted comments if configured
                    if filter_settings.get('exclude_deleted_comments', True) and (
                        comment.author is None or comment.body == '[deleted]'
                    ):
                        continue
                        
                    comment_data = {
                        'rank': i,
                        'author': str(comment.author) if comment.author else '[deleted]',
                        'body': clean_text(comment.body),
                        'score': comment.score,
                        'created_utc': datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                    }
                    comments_data.append(comment_data)
            else:
                comments_data = []
            
            # Extract post data
            post_data = {
                'keyword': keyword,
                'subreddit_searched': subreddit_name,
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
                'images': extract_images_from_post(submission) if export_settings.get('include_images', True) else "",
                'is_video': submission.is_video,
                'over_18': submission.over_18,
                'comments': comments_data
            }
            
            all_posts_data.append(post_data)
            post_count += 1
            
            # Small delay between posts based on config
            time.sleep(api_settings.get('post_processing_delay', 0.5))
            
        except Exception as e:
            print(f"Error processing post: {e}")
            continue

def export_to_json(posts_data, filename=None):
    """Export posts and comments data to JSON"""
    if not posts_data:
        print("No data to export.")
        return
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reddit_posts_{timestamp}.json"
    
    try:
        # Create a cleaner JSON structure
        export_data = {
            "export_info": {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "total_posts": len(posts_data),
                "script_version": "1.1"
            },
            "posts": []
        }
        
        for post in posts_data:
            post_json = {
                "search_info": {
                    "keyword": post['keyword'],
                    "subreddit_searched": post['subreddit_searched']
                },
                "post_details": {
                    "subreddit": post['subreddit'],
                    "title": post['title'],
                    "body": post['body'],
                    "author": post['author'],
                    "score": post['score'],
                    "upvote_ratio": post['upvote_ratio'],
                    "num_comments": post['num_comments'],
                    "created_utc": post['created_utc'],
                    "url": post['url'],
                    "permalink": post['permalink'],
                    "images": post['images'].split('; ') if post['images'] else [],
                    "is_video": post['is_video'],
                    "over_18": post['over_18']
                },
                "top_comments": post['comments']
            }
            export_data["posts"].append(post_json)
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(export_data, jsonfile, indent=2, ensure_ascii=False)
        
        print(f"Data exported to {filename}")
        print(f"Total posts processed: {len(posts_data)}")
        
    except Exception as e:
        print(f"Error exporting to JSON: {e}")

def export_to_csv(posts_data, filename=None):
    """Export posts and comments data to CSV"""
    if not posts_data:
        print("No data to export.")
        return
    
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reddit_posts_{timestamp}.csv"
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'keyword', 'subreddit_searched', 'subreddit', 'title', 'body', 'author',
                'score', 'upvote_ratio', 'num_comments', 'created_utc', 'url', 
                'permalink', 'images', 'is_video', 'over_18',
                'comment_rank', 'comment_author', 'comment_body', 'comment_score', 'comment_created_utc'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for post in posts_data:
                # If post has comments, write one row per comment
                if post['comments']:
                    for comment in post['comments']:
                        row = {
                            'keyword': post['keyword'],
                            'subreddit_searched': post['subreddit_searched'],
                            'subreddit': post['subreddit'],
                            'title': post['title'],
                            'body': post['body'],
                            'author': post['author'],
                            'score': post['score'],
                            'upvote_ratio': post['upvote_ratio'],
                            'num_comments': post['num_comments'],
                            'created_utc': post['created_utc'],
                            'url': post['url'],
                            'permalink': post['permalink'],
                            'images': post['images'],
                            'is_video': post['is_video'],
                            'over_18': post['over_18'],
                            'comment_rank': comment['rank'],
                            'comment_author': comment['author'],
                            'comment_body': comment['body'],
                            'comment_score': comment['score'],
                            'comment_created_utc': comment['created_utc']
                        }
                        writer.writerow(row)
                else:
                    # If no comments, write post data only
                    row = {
                        'keyword': post['keyword'],
                        'subreddit_searched': post['subreddit_searched'],
                        'subreddit': post['subreddit'],
                        'title': post['title'],
                        'body': post['body'],
                        'author': post['author'],
                        'score': post['score'],
                        'upvote_ratio': post['upvote_ratio'],
                        'num_comments': post['num_comments'],
                        'created_utc': post['created_utc'],
                        'url': post['url'],
                        'permalink': post['permalink'],
                        'images': post['images'],
                        'is_video': post['is_video'],
                        'over_18': post['over_18'],
                        'comment_rank': '',
                        'comment_author': '',
                        'comment_body': '',
                        'comment_score': '',
                        'comment_created_utc': ''
                    }
                    writer.writerow(row)
        
        print(f"Data exported to {filename}")
        print(f"Total posts processed: {len(posts_data)}")
        
    except Exception as e:
        print(f"Error exporting to CSV: {e}")

def export_data(posts_data, export_format="csv", filename=None):
    """
    Export data in the specified format
    
    Args:
        posts_data: List of post data dictionaries
        export_format: "csv" or "json"
        filename: Optional custom filename
    """
    if export_format.lower() == "json":
        export_to_json(posts_data, filename)
    elif export_format.lower() == "csv":
        export_to_csv(posts_data, filename)
    else:
        print(f"Unsupported export format: {export_format}")
        print("Supported formats: 'csv', 'json'")
        return False
    return True

def main():
    """Main function to run the Reddit scraper"""
    
    # Load configuration
    config = load_config("config.json")
    if not config:
        print("❌ Failed to load configuration. Exiting.")
        return
    
    # Validate configuration
    if not validate_config(config):
        print("❌ Configuration validation failed. Please check your config.json file.")
        return
    
    # Print configuration summary
    print_config_summary(config)
    
    # Search for posts
    posts_data = search_reddit_posts(config)
    
    # Export to chosen format
    export_settings = config['export_settings']
    if posts_data:
        export_data(
            posts_data, 
            export_settings['format'], 
            export_settings['custom_filename']
        )
        print(f"\n✅ Scraping completed! Found {len(posts_data)} posts total.")
    else:
        print("❌ No posts found matching the criteria.")

if __name__ == "__main__":
    main()