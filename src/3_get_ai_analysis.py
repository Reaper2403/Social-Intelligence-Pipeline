import os
import json
import time
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

# --- MODIFIED: Define the data directory ---
DATA_DIR = "data"

# --- Configuration ---
API_KEY = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY_HERE")
MODEL_NAME = "gpt-4o"

# --- SAFER BATCHING ---
BATCH_SIZE = 5
DELAY_BETWEEN_REQUESTS = 5 

# --- MODIFIED: Construct full paths for all files ---
SYSTEM_PROMPT_FILE = os.path.join(DATA_DIR, "system_prompt_final.txt")
INPUT_DATA_FILE = os.path.join(DATA_DIR, "ai_input_minimal.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "ai_analysis_output.json")


# --- JSON SCHEMA DEFINITION ---
# This schema is our "blueprint" for the AI's response.
JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "analyses": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "opportunity_id": {"type": "string"},
                    "status": {"type": "string", "enum": ["Suitable", "Unsuitable"]},
                    "reason": {"type": ["string", "null"]},
                    "conversation_theme": {"type": ["string", "null"]},
                    "relevant_philosophy": {"type": ["string", "null"]},
                    "strategic_direction": {"type": ["string", "null"]}
                },
                "required": ["opportunity_id", "status", "reason", "conversation_theme", "relevant_philosophy", "strategic_direction"]
            }
        }
    },
    "required": ["analyses"]
}

def run_ai_analysis():
    print("--- Starting OpenAI Analysis (Schema Enforced Mode) ---")

    if not API_KEY or API_KEY == "YOUR_OPENAI_API_KEY_HERE":
        print("ERROR: OpenAI API key not found.")
        return
    client = OpenAI(api_key=API_KEY)

    try:
        with open(SYSTEM_PROMPT_FILE, 'r', encoding='utf-8') as f:
            system_prompt = f.read()
        with open(INPUT_DATA_FILE, 'r', encoding='utf-8') as f:
            all_opportunities = json.load(f)
        print(f"Successfully loaded {len(all_opportunities)} opportunities.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"ERROR: Could not load required files. Details: {e}")
        return

    batches = [all_opportunities[i:i + BATCH_SIZE] for i in range(0, len(all_opportunities), BATCH_SIZE)]
    print(f"Data split into {len(batches)} batches of up to {BATCH_SIZE} items each.")
    
    all_analyses = []

    for i, batch in enumerate(batches):
        print(f"\nProcessing Batch {i + 1} of {len(batches)}...")
        
        # We construct a user message that contains the schema and the data
        user_content_payload = {
            "instructions": "Analyze the data_batch provided and return the output in a JSON object strictly following the json_schema.",
            "json_schema": JSON_SCHEMA,
            "data_batch": batch
        }
        # The user content sent to the API is a string
        user_content = json.dumps(user_content_payload)

        try:
            completion = client.chat.completions.create(
                model=MODEL_NAME,
                response_format={"type": "json_object"}, 
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ]
            )
            response_content_str = completion.choices[0].message.content
            
            parsed_response = json.loads(response_content_str)
            json_array = parsed_response.get("analyses")

            if json_array is None or not isinstance(json_array, list):
                raise ValueError("AI response did not contain the expected 'analyses' array.")

            all_analyses.extend(json_array)
            print(f"Batch {i + 1} successfully processed. Received {len(json_array)} analyses.")

        except Exception as e:
            print(f"An error occurred during API call or parsing for Batch {i + 1}: {e}")
            if 'response_content_str' in locals():
                print("\n--- RAW AI RESPONSE (FOR DEBUGGING) ---")
                print(response_content_str)
                print("-----------------------------------------\n")
            print("Skipping this batch.")
            continue

        if i < len(batches) - 1:
            print(f"Waiting for {DELAY_BETWEEN_REQUESTS} seconds...")
            time.sleep(DELAY_BETWEEN_REQUESTS)

    if all_analyses:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_analyses, f, indent=2)
        print(f"\n--- Analysis Complete ---")
        print(f"Successfully saved {len(all_analyses)} total analyses to '{OUTPUT_FILE}'.")
    else:
        print("\n--- Analysis Failed ---")
        print("No analyses were successfully completed.")

if __name__ == "__main__":
    run_ai_analysis()