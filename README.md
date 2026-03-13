# The Reel Collection

A data pipeline and Streamlit app that surfaces high-rated films you can't find on Netflix, Prime, Disney+, or Apple TV+.

Built as part of the Moviever PoC — a physical rental shelf curated by real audience ratings, not marketing.

## How it works

1. Run `pipeline.py` to fetch and filter films from TMDB
2. Open the Streamlit app to browse the shelf

## Setup

```bash
pip install -r requirements.txt
```

Add a `.env` file with your TMDB Bearer token:

```
TMDB_API_TOKEN=your_token_here
```

## Running

```bash
# Run the pipeline
python pipeline.py

# Start the app
streamlit run app.py
```

## Pipeline

Fetches 200 top-rated films from TMDB, enriches each with runtime, director, UK streaming availability, and keywords — then filters to:

- Rating ≥ 7.0
- Votes ≥ 500
- Runtime ≥ 40 min
- Not on Netflix, Prime, Disney+, or Apple TV+

Output saved to `reel_collection.csv`.

## Team

Abbas · Tom · Dom — Digital Futures
