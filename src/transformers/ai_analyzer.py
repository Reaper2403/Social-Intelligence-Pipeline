"""
Transformer: AI Analyzer
-------------------------
Sends batched Reddit engagement opportunities to a Claude model for structured
analysis, using tool_use to guarantee schema-compliant JSON output.

This is a pure transformer — it receives a list of opportunity dicts and
returns a list of analysis dicts. All disk I/O is handled by the caller.

Public API
----------
    from src.transformers.ai_analyzer import AIAnalyzer

    analyzer = AIAnalyzer(system_prompt_path)
    analyses = analyzer.run(ai_input_list)
"""

import json
import logging
import time
from pathlib import Path

import anthropic
from anthropic import APIConnectionError, APIStatusError, RateLimitError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Anthropic tool schema — matches the original pipeline exactly
# ---------------------------------------------------------------------------
_ANALYSIS_TOOL = {
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
                        "opportunity_id":      {"type": "string"},
                        "status":              {"type": "string", "enum": ["Suitable", "Unsuitable"]},
                        "reason":              {"type": ["string", "null"]},
                        "conversation_theme":  {"type": ["string", "null"]},
                        "relevant_philosophy": {"type": ["string", "null"]},
                        "strategic_direction": {"type": ["string", "null"]},
                    },
                    "required": [
                        "opportunity_id", "status", "reason",
                        "conversation_theme", "relevant_philosophy", "strategic_direction",
                    ],
                },
            }
        },
        "required": ["analyses"],
    },
}


class AIAnalyzer:
    """
    Sends opportunity batches to Claude and returns structured analyses.

    Args:
        system_prompt_path: Path to the system prompt text file
        model:              Anthropic model name
        batch_size:         Number of opportunities per API call
        delay_between:      Seconds to wait between batch calls
    """

    def __init__(
        self,
        system_prompt_path: Path,
        model: str = "claude-sonnet-4-6",
        batch_size: int = 5,
        delay_between: int = 5,
    ):
        self.model = model
        self.batch_size = batch_size
        self.delay_between = delay_between
        self._client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env

        try:
            with open(system_prompt_path, "r", encoding="utf-8") as f:
                self._system_prompt = f.read()
            logger.info(f"System prompt loaded from {system_prompt_path}")
        except FileNotFoundError:
            raise FileNotFoundError(f"System prompt not found: {system_prompt_path}")

    def run(self, opportunities: list[dict]) -> list[dict]:
        """
        Analyse a list of opportunity dicts.

        Args:
            opportunities: list of dicts from OpportunityFilter.run()

        Returns:
            list of analysis dicts (each containing opportunity_id, status, etc.)
        """
        if not opportunities:
            logger.info("No opportunities to analyse.")
            return []

        batches = [
            opportunities[i : i + self.batch_size]
            for i in range(0, len(opportunities), self.batch_size)
        ]
        logger.info(f"Analysing {len(opportunities)} opportunities in {len(batches)} batch(es).")

        all_analyses: list[dict] = []

        for idx, batch in enumerate(batches):
            logger.info(f"Batch {idx + 1}/{len(batches)} …")
            user_content = json.dumps({
                "instructions": (
                    "Analyze the data_batch provided and call the "
                    "'submit_opportunity_analyses' tool with your results."
                ),
                "data_batch": batch,
            })

            try:
                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=self._system_prompt,
                    tools=[_ANALYSIS_TOOL],
                    tool_choice={"type": "tool", "name": "submit_opportunity_analyses"},
                    messages=[{"role": "user", "content": user_content}],
                )

                tool_block = next(
                    (b for b in response.content if b.type == "tool_use"), None
                )
                if tool_block is None:
                    raise ValueError("No tool_use block in response.")

                analyses = tool_block.input.get("analyses")
                if not isinstance(analyses, list):
                    raise ValueError("'analyses' key missing or not a list.")

                all_analyses.extend(analyses)
                logger.info(f"  → {len(analyses)} analyses received.")

            except RateLimitError as e:
                logger.error(f"Rate limit on batch {idx + 1}: {e}. Skipping.")
            except APIConnectionError as e:
                logger.error(f"Connection error on batch {idx + 1}: {e}. Skipping.")
            except APIStatusError as e:
                logger.error(f"API {e.status_code} on batch {idx + 1}: {e.message}. Skipping.")
            except (ValueError, KeyError) as e:
                logger.error(f"Parse error on batch {idx + 1}: {e}. Skipping.")

            if idx < len(batches) - 1:
                logger.info(f"  Waiting {self.delay_between}s before next batch …")
                time.sleep(self.delay_between)

        logger.info(f"AI analysis complete — {len(all_analyses)} results total.")
        return all_analyses
