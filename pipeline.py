import os
import requests
import pandas as pd
from prefect import flow, task
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")


if not TMDB_API_TOKEN:
    raise ValueError("TMDB_API_TOKEN not found. Check your .env file.")

# ── CONFIG ────────────────────────────────────────────────────────────────────

HEADERS = {"Authorization": f"Bearer {TMDB_API_TOKEN}"}
BASE_URL = "https://api.themoviedb.org/3"

MAJOR_PLATFORMS = {8, 9, 337, 350}  # Netflix, Prime, Disney+, Apple TV+
MIN_RATING = 7.0
MIN_VOTES = 500
OUTPUT_PATH = "reel_collection.csv"


# ── TASKS ─────────────────────────────────────────────────────────────────────

@task(retries=2)
def fetch_movies(pages: int = 3) -> list[dict]:
    movies = []
    for page in range(1, pages + 1):
        r = requests.get(f"{BASE_URL}/movie/top_rated", headers=HEADERS, params={"page": page})
        r.raise_for_status()
        movies.extend(r.json().get("results", []))
    print(f"Fetched {len(movies)} films")
    return movies

@task(retries=2, name="Fetch Genre Map")
def fetch_genre_map() -> dict:
    r = requests.get(f"{BASE_URL}/genre/movie/list", headers=HEADERS)
    genres = r.json().get("genres", [])
    return {g["id"]: g["name"] for g in genres}

@task(retries=2, name="Fetch Keywords")
def fetch_keywords(movies: list[dict]) -> list[dict]:
    for movie in movies:
        try:
            r = requests.get(f"{BASE_URL}/movie/{movie['id']}/keywords", headers=HEADERS)
            keywords = [k["name"] for k in r.json().get("keywords", [])]
            movie["keywords"] = ", ".join(keywords[:5])
        except Exception:
            movie["keywords"] = ""
    return movies


@task(retries=2)
def add_streaming_info(movies: list[dict]) -> list[dict]:
    for movie in movies:
        try:
            r = requests.get(f"{BASE_URL}/movie/{movie['id']}/watch/providers", headers=HEADERS)
            providers = r.json().get("results", {}).get("GB", {}).get("flatrate", [])
        except Exception:
            providers = []
        movie["streaming_platforms"] = [p["provider_name"] for p in providers]
        movie["on_major_platform"] = bool({p["provider_id"] for p in providers} & MAJOR_PLATFORMS)
    return movies


@task(name="Filter & Save")
def filter_and_save(movies: list[dict], genre_map: dict) -> str:
    df = pd.DataFrame(movies)[["title", "release_date", "vote_average", "vote_count", "overview", "genre_ids", "poster_path", "keywords", "streaming_platforms", "on_major_platform"]]
    df["poster_path"] = df["poster_path"].fillna("")
    df["year"] = pd.to_datetime(df["release_date"], errors="coerce").dt.year
    df["genres"] = df["genre_ids"].apply(lambda ids: ", ".join([genre_map.get(i, "") for i in ids]))
    df["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

    reel = df[
        (df["vote_average"] >= MIN_RATING) &
        (df["vote_count"] >= MIN_VOTES) &
        (df["on_major_platform"] == False)
    ].sort_values("vote_average", ascending=False)

    if reel.empty:
        print("Warning: no films passed the filter — CSV not updated")
        return OUTPUT_PATH

    reel.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(reel)} films to {OUTPUT_PATH} out of {len(df)} fetched")
    return OUTPUT_PATH

# ── FLOW ──────────────────────────────────────────────────────────────────────

@flow(name="Reel Collection Pipeline", log_prints=True)
def reel_collection_pipeline():
    movies = fetch_movies()
    genre_map = fetch_genre_map()
    movies = add_streaming_info(movies)
    movies = fetch_keywords(movies)
    filter_and_save(movies, genre_map)


if __name__ == "__main__":
    reel_collection_pipeline()