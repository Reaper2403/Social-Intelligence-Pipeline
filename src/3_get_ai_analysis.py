"""
Stage 3: AI Opportunity Analysis
---------------------------------
Sends batched Reddit engagement opportunities to Claude for structured analysis.
Uses tool_use with a forced tool call to guarantee schema-compliant JSON output —
no fragile string parsing required.

Inputs:  data/ai_input_minimal.json, data/system_prompt_final.txt, .env (ANTHROPIC_API_KEY)
Outputs: data/ai_analysis_output.json

Usage:
    python src/3_get_ai_analysis.py
"""

import json
import logging
import time
from pathlib import Path

import anthropic
from anthropic import APIConnectionError, RateLimitError, APIStatusError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Data directory relative to this script's location ---
DATA_DIR = Path(__file__).parent.parent / "data"

MODEL_NAME = "claude-sonnet-4-6"
BATCH_SIZE = 5
DELAY_BETWEEN_REQUESTS = 5

SYSTEM_PROMPT_FILE = DATA_DIR / "system_prompt_final.txt"
INPUT_DATA_FILE = DATA_DIR / "ai_input_minimal.json"
OUTPUT_FILE = DATA_DIR / "ai_analysis_output.json"

# --- Tool definition: schema matches the original pipeline JSON_SCHEMA exactly ---
ANALYSIS_TOOL = {
    "name": "submit_opportunity_analyses",
    "description": (
        "Submit structured analyses for a batch of Reddit engagement opportunities. "
        "Call this tool once with all analyses for the provided batch."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "analyses": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "opportunity_id":    {"type": "string"},
                        "status":            {"type": "string", "enum": ["Suitable", "Unsuitable"]},
                        "reason":            {"type": ["string", "null"]},
                        "conversation_theme":{"type": ["string", "null"]},
                        "relevant_philosophy":{"type": ["string", "null"]},
                        "strategic_direction":{"type": ["string", "null"]}
                    },
                    "required": [
                        "opportunity_id", "status", "reason",
                        "conversation_theme", "relevant_philosophy", "strategic_direction"
                    ]
                }
            }
        },
        "required": ["analyses"]
    }
}


def run_ai_analysis():
    logger.info("Starting Claude Analysis (Tool-Enforced Structured Output)")

    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env automatically

    try:
        with open(SYSTEM_PROMPT_FILE, 'r', encoding='utf-8') as f:
            system_prompt = f.read()
        with open(INPUT_DATA_FILE, 'r', encoding='utf-8') as f:
            all_opportunities = json.load(f)
        logger.info(f"Loaded {len(all_opportunities)} opportunities.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Could not load required files: {e}")
        return

    batches = [
        all_opportunities[i:i + BATCH_SIZE]
        for i in range(0, len(all_opportunities), BATCH_SIZE)
    ]
    logger.info(f"Split into {len(batches)} batches of up to {BATCH_SIZE} items each.")

    all_analyses = []

    for i, batch in enumerate(batches):
        logger.info(f"Processing Batch {i + 1} of {len(batches)}...")

        user_content = json.dumps({
            "instructions": (
                "Analyze the data_batch provided and call the "
                "'submit_opportunity_analyses' tool with your results."
            ),
            "data_batch": batch
        })

        try:
            response = client.messages.create(
                model=MODEL_NAME,
                max_tokens=4096,
                system=system_prompt,
                tools=[ANALYSIS_TOOL],
                tool_choice={"type": "tool", "name": "submit_opportunity_analyses"},
                messages=[{"role": "user", "content": user_content}]
            )

            # Extract the tool_use block — input is already a Python dict, no json.loads needed
            tool_use_block = next(
                (b for b in response.content if b.type == "tool_use"),
                None
            )
            if tool_use_block is None:
                raise ValueError("No tool_use block found in response.")

            json_array = tool_use_block.input.get("analyses")
            if json_array is None or not isinstance(json_array, list):
                raise ValueError("Tool input did not contain an 'analyses' array.")

            all_analyses.extend(json_array)
            logger.info(f"Batch {i + 1} complete — received {len(json_array)} analyses.")

        except RateLimitError as e:
            logger.error(f"Rate limit hit on Batch {i + 1}: {e}. Skipping.")
            continue
        except APIConnectionError as e:
            logger.error(f"Connection error on Batch {i + 1}: {e}. Skipping.")
            continue
        except APIStatusError as e:
            logger.error(f"API status error {e.status_code} on Batch {i + 1}: {e.message}. Skipping.")
            continue
        except (ValueError, KeyError) as e:
            logger.error(f"Parsing error on Batch {i + 1}: {e}. Skipping.")
            continue

        if i < len(batches) - 1:
            logger.info(f"Waiting {DELAY_BETWEEN_REQUESTS}s before next batch...")
            time.sleep(DELAY_BETWEEN_REQUESTS)

    if all_analyses:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_analyses, f, indent=2)
        logger.info(f"Analysis complete. Saved {len(all_analyses)} analyses to '{OUTPUT_FILE}'.")
    else:
        logger.error("Analysis failed — no analyses were successfully completed.")


if __name__ == "__main__":
    run_ai_analysis()
