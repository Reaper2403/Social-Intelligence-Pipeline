# Reddit Engagement Analysis Pipeline

An automated 4-stage pipeline that scrapes Reddit discussions from South Asian dating communities, scores engagement opportunities, runs AI coaching analysis via Claude, and generates actionable Word-document briefings.

---

## Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    REDDIT ANALYSIS PIPELINE                         │
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │ Stage 1  │───▶│ Stage 2  │───▶│ Stage 3  │───▶│  Stage 4     │  │
│  │  Fetch   │    │  Filter  │    │ Analyze  │    │  Report      │  │
│  │  Reddit  │    │   & Rank │    │ (Claude) │    │ (Word Docs)  │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│       │                │                │                │          │
│  PRAW API        95th pct rank    tool_use JSON    .docx briefings  │
│  + scoring       deduplication    structured out   with hyperlinks  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Library | Purpose |
|-----------|---------|---------|
| Reddit API | `praw >= 7.8.1` | Scrape posts & comments |
| AI Analysis | `anthropic >= 0.40.0` | Claude Sonnet structured analysis |
| Data Processing | `pandas >= 2.3.0`, `numpy >= 2.0.0` | Scoring, filtering |
| Report Generation | `python-docx >= 1.1.0` | Word documents with hyperlinks |
| EDA Notebook | `jupyterlab`, `seaborn`, `matplotlib`, `wordcloud` | Portfolio visualisations |
| Environment | `python-dotenv >= 1.0.0` | API key management |

---

## Opportunity Scoring Formula

Posts and comments are ranked by a velocity-based **Opportunity Score** that rewards active, recent discussions.

**Post score:**

```
OS_post = (W1 × (score × upvote_ratio) + W2 × num_comments) / (age_hours + S)
```

**Reply score:**

```
OS_reply = OS_post + (W3 × comment_score + W4 × num_replies) × (1 / (depth + 1))
```

Default weights: `W1=1.0`, `W2=1.5`, `W3=1.0`, `W4=2.0`, `S=2` (smoothing).
Only the **top 5% (95th percentile)** of scored opportunities are forwarded to the AI stage.

---

## AI Analysis Framework

Claude analyses each opportunity against six coaching philosophies:

1. **Courtship is a Dance, Not a Chase** — mutual effort and reciprocity
2. **Emotional Honesty + Social Grace** — authentic expression within social norms
3. **Redefining Modern Masculinity** — challenging outdated gender narratives
4. **The Long Game** — investing in personal growth over quick wins
5. **Cultural Intelligence** — navigating South Asian cultural expectations
6. **Abundance Mindset** — approaching dating from a place of confidence, not scarcity

Each opportunity receives: `status` (Suitable/Unsuitable), `conversation_theme`, `relevant_philosophy`, and `strategic_direction`.

---

## Project Structure

```
reddit-analysis-pipeline/
├── src/
│   ├── 1_fetch_reddit_data.py    # Stage 1: Scrape & score Reddit posts
│   ├── 2_prepare_ai_input.py     # Stage 2: Filter top 5%, deduplicate
│   ├── 3_get_ai_analysis.py      # Stage 3: Claude AI coaching analysis
│   └── 4_generate_reports.py     # Stage 4: Word document reports
├── data/
│   ├── config.json               # Search settings (subreddits, keywords)
│   └── system_prompt_final.txt   # Claude coaching system prompt
├── notebooks/
│   └── eda.ipynb                 # Exploratory Data Analysis (8 sections)
├── reports/
│   └── .gitkeep                  # Generated .docx reports go here
├── Main_controller.py            # Interactive pipeline runner
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

```bash
# 1. Clone
git clone https://github.com/your-username/reddit-analysis-pipeline.git
cd reddit-analysis-pipeline

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
cp .env.example .env
# Edit .env with your Reddit and Anthropic API keys
```

**Reddit API credentials** — create an app at https://www.reddit.com/prefs/apps (script type).
**Anthropic API key** — obtain at https://console.anthropic.com.

---

## Usage

### Full pipeline (interactive menu)

```bash
python Main_controller.py
```

Select a starting step (1–4). The controller checks for required input files and prompts for confirmation before making the Anthropic API call.

### Individual stages

```bash
python src/1_fetch_reddit_data.py     # Scrape Reddit → data/master_reddit_data.json
python src/2_prepare_ai_input.py      # Filter → data/ai_input_minimal.json
python src/3_get_ai_analysis.py       # Analyze → data/ai_analysis_output.json
python src/4_generate_reports.py      # Report → reports/Report_Posts.docx + Report_Comments.docx
```

### EDA Notebook

```bash
jupyter lab notebooks/eda.ipynb
```

---

## EDA Notebook

[`notebooks/eda.ipynb`](notebooks/eda.ipynb) — 8-section analysis of the 576-post dataset:

| Section | Content |
|---------|---------|
| 1. Dataset Overview | `.describe()` table, shape, date range |
| 2. Subreddit Distribution | Post count & mean score per subreddit |
| 3. Keyword Analysis | Top 15 keywords + word cloud |
| 4. Engagement Metrics | Score, upvote ratio, comments, age distributions |
| 5. Opportunity Score Analysis | Histogram, scatter plots, threshold line |
| 6. Temporal Patterns | Weekly volume + day-of-week bar chart |
| 7. AI Analysis Results | Suitable/Unsuitable pie + philosophy bar chart |
| 8. Key Findings | Summary table + 5 written insights |

---

## Results Summary

| Metric | Value |
|--------|-------|
| Total posts collected | 576 |
| Subreddits covered | SouthAsianMasculinity, SouthAsians, datingadvice, and others |
| Opportunities sent to AI (top 5%) | ~29 post + comment opportunities per run |
| AI model | Claude Sonnet (`claude-sonnet-4-6`) |
| Structured output method | `tool_use` with forced tool call (no JSON parsing) |
| Report format | Word (.docx) with clickable Reddit hyperlinks |

---

## License

MIT License — see [LICENSE](LICENSE) for details.
