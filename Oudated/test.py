import praw
import csv
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

def search_reddit_posts(keywords, subreddits=None, limit=5, sort_method="top", time_filter="week"):
    """
    Search Reddit for posts containing keywords
    
    Args:
        keywords: List of keywords to search for
        subreddits: List of subreddits to search in (None for all)
        limit: Number of posts to fetch per keyword
        sort_method: Sort method (top, hot, new, controversial)
        time_filter: Time filter for top/controversial posts (hour, day, week, month, year, all)
    """
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
                        process_posts(posts, keyword, subreddit_name, all_posts_data, limit)
                    except Exception as e:
                        print(f"Error searching in r/{subreddit_name}: {e}")
            else:
                # Search all of Reddit
                try:
                    all_subreddit = reddit.subreddit("all")
                    posts = get_posts_by_method(all_subreddit, keyword, sort_method, time_filter, limit)
                    process_posts(posts, keyword, "all", all_posts_data, limit)
                except Exception as e:
                    print(f"Error searching all of Reddit for '{keyword}': {e}")
            
            # Add delay to respect rate limits
            time.sleep(1)
                    
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

def process_posts(posts, keyword, subreddit_name, all_posts_data, limit):
    """Process posts and extract data"""
    post_count = 0
    
    for submission in posts:
        if post_count >= limit:
            break
            
        try:
            print(f"Processing post: {submission.title[:50]}...")
            
            # Get top comments
            submission.comments.replace_more(limit=0)  # Remove "more comments"
            top_comments = sorted(submission.comments, key=lambda x: x.score, reverse=True)[:10]
            
            comments_data = []
            for i, comment in enumerate(top_comments, 1):
                comment_data = {
                    'rank': i,
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'body': clean_text(comment.body),
                    'score': comment.score,
                    'created_utc': datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                }
                comments_data.append(comment_data)
            
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
                'images': extract_images_from_post(submission),
                'is_video': submission.is_video,
                'over_18': submission.over_18,
                'comments': comments_data
            }
            
            all_posts_data.append(post_data)
            post_count += 1
            
            # Small delay between posts
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error processing post: {e}")
            continue

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

def main():
    """Main function to run the Reddit scraper"""
    
    # ✅ CONFIGURATION - EDIT HERE
    keywords = ["dating", "south asian", "south asian dating","South asian men","desi dating", "talk to girls","women","advice","relationship","dating advice","dating tips","dating problems","dating issues"]
    
    # Option 1: Search specific subreddits
    target_subreddits = ["SouthAsianMasculinity","TooAfraidToAsk","ABCDesi","SouthAsian","AsianMasculinity","SouthAsianDating","SouthAsianMen","DatinginInIndia"]
    
    # Option 2: Search all of Reddit (set to None)
    # target_subreddits = None
    
    posts_per_keyword = 2  # Number of posts per keyword
    
    # ✅ SORTING OPTIONS - Choose one:
    sort_method = "hot"
    # sort_method = "top"
    # sort_method = "hot"
    # sort_method = "new"
    # sort_method = "controversial"

    # ✅ TIME FILTER - Only applies to "top" and "controversial" sorting
    # Options: "hour", "day", "week", "month", "year", "all"
    time_filter = "week"       # Ignored for "hot" and "new" sorting
    
    print("Starting Reddit post search...")
    print(f"Keywords: {keywords}")
    print(f"Target subreddits: {target_subreddits}")
    print(f"Posts per keyword: {posts_per_keyword}")
    print(f"Sort method: {sort_method}")
    if sort_method in ["top", "controversial"]:
        print(f"Time filter: {time_filter}")
    else:
        print("Time filter: Not applicable (using hot/new sorting)")
    print("-" * 50)
    
    # Search for posts
    posts_data = search_reddit_posts(
        keywords=keywords,
        subreddits=target_subreddits,
        limit=posts_per_keyword,
        sort_method=sort_method,
        time_filter=time_filter
    )
    
    # Export to CSV
    if posts_data:
        export_to_csv(posts_data)
        print(f"\nScraping completed! Found {len(posts_data)} posts total.")
    else:
        print("No posts found matching the criteria.")

if __name__ == "__main__":
    main()