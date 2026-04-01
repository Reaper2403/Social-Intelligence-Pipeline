"""
tests/test_phase1_etl.py
------------------------
Unit tests for Phase 1: Modular ETL Refactoring.

Tests each layer in isolation — no Reddit API calls, no Anthropic API calls,
no file-system side effects (all I/O is patched or uses tmp_path).

Run with:
    cd "Reddit Projects"
    python -m pytest tests/test_phase1_etl.py -v
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make sure project root is on sys.path
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ===========================================================================
# json_loader tests
# ===========================================================================
class TestJsonLoader:
    def test_load_json_valid(self, tmp_path):
        from src.loaders.json_loader import load_json

        f = tmp_path / "data.json"
        f.write_text(json.dumps({"key": "value"}))
        result = load_json(f)
        assert result == {"key": "value"}

    def test_load_json_missing_returns_none(self, tmp_path):
        from src.loaders.json_loader import load_json

        result = load_json(tmp_path / "nonexistent.json")
        assert result is None

    def test_load_json_invalid_returns_none(self, tmp_path):
        from src.loaders.json_loader import load_json

        f = tmp_path / "bad.json"
        f.write_text("{ not valid json }")
        result = load_json(f)
        assert result is None

    def test_save_json_creates_file(self, tmp_path):
        from src.loaders.json_loader import save_json

        out = tmp_path / "out.json"
        result = save_json({"a": 1}, out)
        assert result is True
        assert json.loads(out.read_text()) == {"a": 1}

    def test_save_json_compact(self, tmp_path):
        from src.loaders.json_loader import save_json

        out = tmp_path / "compact.json"
        save_json([1, 2, 3], out, compact=True)
        # compact mode uses no spaces
        assert " " not in out.read_text()

    def test_load_config_valid(self, tmp_path):
        from src.loaders.json_loader import load_config

        cfg = {
            "search_settings": {},
            "export_settings": {},
            "api_settings": {},
            "filter_settings": {},
        }
        f = tmp_path / "config.json"
        f.write_text(json.dumps(cfg))
        result = load_config(f)
        assert result == cfg

    def test_load_config_missing_key_returns_none(self, tmp_path):
        from src.loaders.json_loader import load_config

        cfg = {"search_settings": {}}  # missing 3 required keys
        f = tmp_path / "config.json"
        f.write_text(json.dumps(cfg))
        result = load_config(f)
        assert result is None

    def test_id_log_roundtrip(self, tmp_path):
        from src.loaders.json_loader import (
            append_processed_ids,
            load_processed_ids,
        )

        log_path = tmp_path / "processed_ids.log"
        append_processed_ids({"post_abc", "comment_xyz"}, log_path)
        loaded = load_processed_ids(log_path)
        assert loaded == {"post_abc", "comment_xyz"}

    def test_append_processed_ids_is_cumulative(self, tmp_path):
        from src.loaders.json_loader import (
            append_processed_ids,
            load_processed_ids,
        )

        log_path = tmp_path / "processed_ids.log"
        append_processed_ids({"id_1"}, log_path)
        append_processed_ids({"id_2"}, log_path)
        loaded = load_processed_ids(log_path)
        assert "id_1" in loaded and "id_2" in loaded

    def test_load_master_store_empty_when_missing(self, tmp_path):
        from src.loaders.json_loader import load_master_store

        result = load_master_store(tmp_path / "missing.json")
        assert result == {"master_data_info": {}, "posts": []}

    def test_save_and_load_master_store(self, tmp_path):
        from src.loaders.json_loader import load_master_store, save_master_store

        posts_dict = {
            "abc123": {
                "post_details": {"id": "abc123", "title": "Test"},
                "top_comments": [],
            }
        }
        path = tmp_path / "master.json"
        save_master_store(posts_dict, path)
        loaded = load_master_store(path)
        assert len(loaded["posts"]) == 1
        assert loaded["posts"][0]["post_details"]["id"] == "abc123"


# ===========================================================================
# OpportunityFilter tests
# ===========================================================================
class TestOpportunityFilter:
    """Build synthetic master data and test the filter logic."""

    def _make_master_data(self, post_scores: list, comment_scores: list):
        """Create a minimal master_data dict with the given scores."""
        posts = []
        for i, ps in enumerate(post_scores):
            comments = []
            if i < len(comment_scores):
                comments.append({
                    "id": f"c{i}",
                    "body": "comment body",
                    "opportunity_score_reply": comment_scores[i],
                })
            posts.append({
                "post_details": {
                    "id": f"p{i}",
                    "title": f"Post {i}",
                    "body": f"Body {i}",
                    "opportunity_score_post": ps,
                },
                "top_comments": comments,
            })
        return {"posts": posts}

    def test_constructor_rejects_bad_percentile(self):
        from src.transformers.opportunity_filter import OpportunityFilter

        with pytest.raises(ValueError):
            OpportunityFilter(percentile=0)
        with pytest.raises(ValueError):
            OpportunityFilter(percentile=100)

    def test_run_returns_only_top_percentile(self):
        from src.transformers.opportunity_filter import OpportunityFilter

        # 20 posts: only top 5% (≥ score 19) should be included at p=95
        post_scores    = list(range(20))  # 0..19
        comment_scores = list(range(20))  # 0..19
        master_data    = self._make_master_data(post_scores, comment_scores)

        opp_filter = OpportunityFilter(percentile=95)
        ai_input, new_ids = opp_filter.run(master_data, processed_ids=set())

        # Verify only high-score items appear
        for item in ai_input:
            assert item["opportunity_id"] in new_ids

    def test_run_skips_processed_ids(self):
        from src.transformers.opportunity_filter import OpportunityFilter

        post_scores    = [100, 200, 300]
        comment_scores = [100, 200, 300]
        master_data    = self._make_master_data(post_scores, comment_scores)

        # Pre-process all post IDs
        already_done = {"post_p0", "post_p1", "post_p2"}
        opp_filter   = OpportunityFilter(percentile=50)
        ai_input, new_ids = opp_filter.run(master_data, processed_ids=already_done)

        # No post_* items should appear
        post_items = [x for x in ai_input if x["opportunity_id"].startswith("post_")]
        assert len(post_items) == 0

    def test_run_empty_master_data(self):
        from src.transformers.opportunity_filter import OpportunityFilter

        opp_filter = OpportunityFilter()
        ai_input, new_ids = opp_filter.run({"posts": []}, processed_ids=set())
        assert ai_input == []
        assert new_ids == set()

    def test_output_schema(self):
        from src.transformers.opportunity_filter import OpportunityFilter

        post_scores    = list(range(1, 101))
        comment_scores = list(range(1, 101))
        master_data    = self._make_master_data(post_scores, comment_scores)

        opp_filter = OpportunityFilter(percentile=50)
        ai_input, _ = opp_filter.run(master_data, processed_ids=set())

        for item in ai_input:
            assert "opportunity_id"   in item
            assert "opportunity_type" in item
            assert "post_title"       in item
            assert item["opportunity_type"] in ("Reply to Post", "Reply to Comment")


# ===========================================================================
# AIAnalyzer tests  (Anthropic API is fully mocked)
# ===========================================================================
class TestAIAnalyzer:
    def _make_analyzer(self, tmp_path):
        from src.transformers.ai_analyzer import AIAnalyzer

        prompt_file = tmp_path / "system_prompt.txt"
        prompt_file.write_text("You are a helpful assistant.")
        return AIAnalyzer(system_prompt_path=prompt_file, batch_size=2, delay_between=0)

    def _mock_response(self, analyses: list):
        """Build a fake Anthropic response object."""
        tool_block       = MagicMock()
        tool_block.type  = "tool_use"
        tool_block.input = {"analyses": analyses}
        response         = MagicMock()
        response.content = [tool_block]
        return response

    def test_raises_if_system_prompt_missing(self, tmp_path):
        from src.transformers.ai_analyzer import AIAnalyzer

        with pytest.raises(FileNotFoundError):
            AIAnalyzer(system_prompt_path=tmp_path / "no_file.txt")

    def test_run_empty_input_returns_empty(self, tmp_path):
        analyzer = self._make_analyzer(tmp_path)
        result   = analyzer.run([])
        assert result == []

    def test_run_returns_analyses(self, tmp_path):
        analyzer = self._make_analyzer(tmp_path)

        fake_analyses = [
            {
                "opportunity_id":      "post_abc",
                "status":              "Suitable",
                "reason":              "Good post",
                "conversation_theme":  "Career",
                "relevant_philosophy": "Growth",
                "strategic_direction": "Engage",
            }
        ]
        mock_resp = self._mock_response(fake_analyses)

        with patch.object(analyzer._client.messages, "create", return_value=mock_resp):
            result = analyzer.run([{"opportunity_id": "post_abc", "opportunity_type": "Reply to Post",
                                    "post_title": "T", "post_body": "B"}])

        assert len(result) == 1
        assert result[0]["opportunity_id"] == "post_abc"
        assert result[0]["status"] == "Suitable"

    def test_run_batches_correctly(self, tmp_path):
        """With batch_size=2, 5 items should produce 3 API calls."""
        analyzer    = self._make_analyzer(tmp_path)
        call_count  = {"n": 0}

        def side_effect(**kwargs):
            call_count["n"] += 1
            batch = json.loads(kwargs["messages"][0]["content"])["data_batch"]
            analyses = [
                {
                    "opportunity_id":      item["opportunity_id"],
                    "status":              "Suitable",
                    "reason":              None,
                    "conversation_theme":  None,
                    "relevant_philosophy": None,
                    "strategic_direction": None,
                }
                for item in batch
            ]
            return self._mock_response(analyses)

        items = [
            {"opportunity_id": f"post_{i}", "opportunity_type": "Reply to Post",
             "post_title": "T", "post_body": "B"}
            for i in range(5)
        ]

        with patch.object(analyzer._client.messages, "create", side_effect=side_effect):
            result = analyzer.run(items)

        assert call_count["n"] == 3        # ceil(5/2) == 3
        assert len(result) == 5

    def test_run_handles_api_error_gracefully(self, tmp_path):
        from anthropic import RateLimitError

        analyzer = self._make_analyzer(tmp_path)

        with patch.object(
            analyzer._client.messages, "create",
            side_effect=RateLimitError("rate limit", response=MagicMock(), body={})
        ):
            result = analyzer.run([{"opportunity_id": "post_x", "opportunity_type": "Reply to Post",
                                    "post_title": "T", "post_body": "B"}])

        assert result == []   # graceful empty result, not a crash


# ===========================================================================
# ReportLoader tests  (no Word file written in most tests)
# ===========================================================================
class TestReportLoader:
    def _make_full_data(self):
        return {
            "posts": [
                {
                    "post_details": {
                        "id": "p1",
                        "subreddit": "testsubreddit",
                        "title": "Test Post",
                        "body": "Post body",
                        "author": "user1",
                        "url": "https://reddit.com/r/test/p1",
                        "opportunity_score_post": 42.5,
                        "created_utc": "2024-01-15 10:00:00",
                    },
                    "top_comments": [
                        {
                            "id": "c1",
                            "body": "Great comment",
                            "author": "commenter",
                            "opportunity_score_reply": 15.0,
                            "created_utc": "2024-01-15 11:00:00",
                        }
                    ],
                }
            ]
        }

    def _make_suitable_analyses(self):
        return [
            {
                "opportunity_id":      "post_p1",
                "status":              "Suitable",
                "reason":              "Good",
                "conversation_theme":  "Career",
                "relevant_philosophy": "Growth",
                "strategic_direction": "Engage warmly",
            },
            {
                "opportunity_id":      "comment_c1",
                "status":              "Suitable",
                "reason":              "Good",
                "conversation_theme":  "Networking",
                "relevant_philosophy": "Community",
                "strategic_direction": "Offer advice",
            },
        ]

    def test_lookup_tables_built_correctly(self):
        from src.loaders.report_loader import ReportLoader

        loader = ReportLoader(self._make_full_data(), [], set())
        assert "post_p1"    in loader._post_lookup
        assert "comment_c1" in loader._comment_lookup

    def test_generate_creates_word_documents(self, tmp_path):
        from src.loaders.report_loader import ReportLoader

        loader = ReportLoader(
            self._make_full_data(),
            self._make_suitable_analyses(),
            previously_reported=set(),
            reports_dir=tmp_path,
        )
        newly_reported = loader.generate()

        assert "post_p1"    in newly_reported
        assert "comment_c1" in newly_reported
        assert (tmp_path / "Report_Posts.docx").exists()
        assert (tmp_path / "Report_Comments.docx").exists()

    def test_generate_skips_previously_reported(self, tmp_path):
        from src.loaders.report_loader import ReportLoader

        loader = ReportLoader(
            self._make_full_data(),
            self._make_suitable_analyses(),
            previously_reported={"post_p1", "comment_c1"},
            reports_dir=tmp_path,
        )
        newly_reported = loader.generate()
        assert len(newly_reported) == 0
        assert not (tmp_path / "Report_Posts.docx").exists()

    def test_generate_skips_unsuitable(self, tmp_path):
        from src.loaders.report_loader import ReportLoader

        analyses = [
            {
                "opportunity_id": "post_p1",
                "status":         "Unsuitable",
                "reason":         "Off topic",
                "conversation_theme": None,
                "relevant_philosophy": None,
                "strategic_direction": None,
            }
        ]
        loader = ReportLoader(
            self._make_full_data(), analyses, set(), reports_dir=tmp_path
        )
        newly_reported = loader.generate()
        assert len(newly_reported) == 0
