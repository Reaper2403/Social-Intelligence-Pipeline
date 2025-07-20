import json
import numpy as np
import os # <-- MODIFIED: Added os import for path handling

# --- MODIFIED: Define the data directory ---
DATA_DIR = "data"

# --- MODIFIED: Construct full paths for all files ---
INPUT_FILENAME = os.path.join(DATA_DIR, "south_asian_dating_reddit_data.json")
OUTPUT_FILENAME = os.path.join(DATA_DIR, "ai_input_minimal.json")
HISTORY_LOG_FILE = os.path.join(DATA_DIR, "processed_ids.log") # This file will track all processed IDs

def prepare_ai_input_with_tracking():
    """
    Reads full Reddit data, filters for new, high-value opportunities
    (skipping any previously processed IDs), creates a minimized JSON for the AI,
    and updates the history log.
    """
    print("--- Starting Pre-processing with Duplicate Check ---")

    # 1. Load the history of already processed IDs
    try:
        with open(HISTORY_LOG_FILE, 'r', encoding='utf-8') as f:
            processed_ids = set(line.strip() for line in f)
        print(f"Loaded {len(processed_ids)} IDs from the history log.")
    except FileNotFoundError:
        processed_ids = set()
        print("No history log found. A new one will be created.")

    # 2. Load the full JSON data
    try:
        with open(INPUT_FILENAME, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Successfully loaded '{INPUT_FILENAME}'.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not load or parse '{INPUT_FILENAME}'. Error: {e}")
        return

    all_posts = data.get("posts", [])
    post_scores = [p["post_details"]["opportunity_score_post"] for p in all_posts if p.get("post_details", {}).get("opportunity_score_post") is not None]
    comment_scores = [c["opportunity_score_reply"] for p in all_posts for c in p.get("top_comments", []) if c.get("opportunity_score_reply") is not None]

    if not post_scores or not comment_scores:
        print("No opportunity scores found.")
        return

    # Using 95th percentile as an example for high-value targets
    post_score_threshold = np.percentile(post_scores, 95)
    comment_score_threshold = np.percentile(comment_scores, 95)

    print(f"\nCalculated Thresholds (Top 5%):")
    print(f"  - Post Score Threshold: {post_score_threshold:.2f}")
    print(f"  - Comment Score Threshold: {comment_score_threshold:.2f}")

    ai_input_data = []
    new_ids_to_log = set()
    skipped_count = 0

    # 3. Filter for high-score opportunities AND check against history
    for post in all_posts:
        post_details = post.get("post_details", {})
        post_id_str = post_details.get('id')
        
        # Process Post
        if post_id_str:
            opp_id = f"post_{post_id_str}"
            if opp_id in processed_ids:
                skipped_count += 1
                continue # Skip if already processed

            current_post_score = post_details.get("opportunity_score_post")
            if current_post_score and current_post_score > post_score_threshold:
                opportunity = {
                    "opportunity_id": opp_id,
                    "opportunity_type": "Reply to Post",
                    "post_title": post_details.get("title", ""),
                    "post_body": post_details.get("body", "")
                }
                ai_input_data.append(opportunity)
                new_ids_to_log.add(opp_id)

        # Process Comments
        for comment in post.get("top_comments", []):
            comment_id_str = comment.get('id')
            if comment_id_str:
                opp_id = f"comment_{comment_id_str}"
                if opp_id in processed_ids:
                    skipped_count += 1
                    continue # Skip if already processed

                current_comment_score = comment.get("opportunity_score_reply")
                if current_comment_score and current_comment_score > comment_score_threshold:
                    opportunity = {
                        "opportunity_id": opp_id,
                        "opportunity_type": "Reply to Comment",
                        "post_title": post_details.get("title", ""),
                        "post_body": post_details.get("body", ""),
                        "target_comment_text": comment.get("body", "")
                    }
                    ai_input_data.append(opportunity)
                    new_ids_to_log.add(opp_id)
    
    print(f"\nSkipped {skipped_count} opportunities that were already in the history log.")
    print(f"Found {len(ai_input_data)} new high-value opportunities to be sent for analysis.")

    # 4. Save the new, unique opportunities for the AI
    if ai_input_data:
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(ai_input_data, f, separators=(',', ':'))
        print(f"Successfully created token-efficient input file: '{OUTPUT_FILENAME}'")

        # 5. Update the history log with the new IDs that were just sent
        with open(HISTORY_LOG_FILE, 'a', encoding='utf-8') as f:
            for item_id in sorted(list(new_ids_to_log)):
                f.write(f"{item_id}\n")
        print(f"Updated '{HISTORY_LOG_FILE}' with {len(new_ids_to_log)} new IDs.")
    else:
        print("No new opportunities found to process.")


if __name__ == "__main__":
    prepare_ai_input_with_tracking()