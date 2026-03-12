import os
import json
import requests
import pandas as pd
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
from datetime import datetime, timedelta
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
MIN_RUNTIME = 40
CACHE_PATH = "movie_cache.json"
CACHE_EXPIRY_DAYS = 7
LANGUAGE_MAP = {
    "en": "English",
    "it": "Italian",
    "ko": "Korean",
    "ja": "Japanese",
    "pt": "Portuguese",
    "fr": "French",
    "es": "Spanish",
    "ru": "Russian",
    "sv": "Swedish",
    "ar": "Arabic",
    "zh": "Chinese",
    "da": "Danish",
    "lv": "Latvian",
    "cn": "Cantonese"
}
OUTPUT_PATH = "reel_collection.csv"


# ── CACHE ─────────────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)


def is_fresh(entry: dict, keys: list) -> bool:
    if not entry or not all(k in entry for k in keys):
        return False
    cached_at = datetime.fromisoformat(entry["cached_at"])
    return datetime.now() - cached_at < timedelta(days=CACHE_EXPIRY_DAYS)


# ── TASKS ─────────────────────────────────────────────────────────────────────

@task(retries=2)
def fetch_movies(pages: int = 10) -> list[dict]:
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


@task(retries=2, name="Fetch Movie Details")
def fetch_movie_details(movie: dict) -> dict:
    # One request per film using append_to_response instead of three separate calls
    try:
        r = requests.get(
            f"{BASE_URL}/movie/{movie['id']}",
            headers=HEADERS,
            params={"append_to_response": "credits,watch/providers"}
        )
        data = r.json()

        movie["runtime"] = data.get("runtime", None)

        crew = data.get("credits", {}).get("crew", [])
        directors = [p["name"] for p in crew if p["job"] == "Director"]
        movie["director"] = ", ".join(directors)

        providers = data.get("watch/providers", {}).get("results", {}).get("GB", {}).get("flatrate", [])
        movie["streaming_platforms"] = [p["provider_name"] for p in providers]
        movie["on_major_platform"] = bool({p["provider_id"] for p in providers} & MAJOR_PLATFORMS)

    except Exception:
        movie["runtime"] = None
        movie["director"] = ""
        movie["streaming_platforms"] = []
        movie["on_major_platform"] = False

    return movie


@task(retries=2, name="Fetch Keywords")
def fetch_keywords(movie: dict) -> dict:
    try:
        r = requests.get(f"{BASE_URL}/movie/{movie['id']}/keywords", headers=HEADERS)
        movie["keywords"] = ", ".join([k["name"] for k in r.json().get("keywords", [])][:5])
    except Exception:
        movie["keywords"] = ""
    return movie


@task(name="Filter & Save")
def filter_and_save(movies: list[dict], genre_map: dict) -> str:
    df = pd.DataFrame(movies)[["title", "release_date", "vote_average", "vote_count", "overview", "genre_ids", "poster_path", "keywords", "original_language", "runtime", "director", "streaming_platforms", "on_major_platform"]]
    df["poster_path"] = df["poster_path"].fillna("")
    df["year"] = pd.to_datetime(df["release_date"], errors="coerce").dt.year
    df["genres"] = df["genre_ids"].apply(lambda ids: ", ".join([genre_map.get(i, "") for i in ids]))
    df["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["language"] = df["original_language"].map(LANGUAGE_MAP).fillna(df["original_language"])
    df = df.drop(columns=["genre_ids", "original_language"])

    reel = df[
        (df["vote_average"] >= MIN_RATING) &
        (df["vote_count"] >= MIN_VOTES) &
        (df["on_major_platform"] == False) &
        (df["runtime"] >= MIN_RUNTIME)
    ].sort_values("vote_average", ascending=False)

    if reel.empty:
        print("Warning: no films passed the filter — CSV not updated")
        return OUTPUT_PATH

    reel.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(reel)} films to {OUTPUT_PATH} out of {len(df)} fetched")
    return OUTPUT_PATH


# ── FLOW ──────────────────────────────────────────────────────────────────────

@flow(name="Reel Collection Pipeline", log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=20))
def reel_collection_pipeline():
    movies = fetch_movies()
    genre_map = fetch_genre_map.submit()
    cache = load_cache()

    detail_keys = ["runtime", "director", "streaming_platforms", "on_major_platform"]
    keyword_keys = ["keywords"]

    # Only fetch details for films not already in the cache
    stale = [m for m in movies if not is_fresh(cache.get(str(m["id"]), {}), detail_keys)]
    fresh = [m for m in movies if is_fresh(cache.get(str(m["id"]), {}), detail_keys)]

    fetched = [f.result() for f in fetch_movie_details.map(stale)] if stale else []

    for movie in fresh:
        entry = cache[str(movie["id"])]
        for key in detail_keys:
            movie[key] = entry[key]

    for movie in fetched:
        movie_id = str(movie["id"])
        cache[movie_id] = {k: movie[k] for k in detail_keys}
        cache[movie_id]["cached_at"] = datetime.now().isoformat()

    movies = fetched + fresh

    # Same for keywords
    stale_kw = [m for m in movies if not is_fresh(cache.get(str(m["id"]), {}), keyword_keys)]
    fresh_kw = [m for m in movies if is_fresh(cache.get(str(m["id"]), {}), keyword_keys)]

    fetched_kw = [f.result() for f in fetch_keywords.map(stale_kw)] if stale_kw else []

    for movie in fresh_kw:
        movie["keywords"] = cache[str(movie["id"])]["keywords"]

    for movie in fetched_kw:
        movie_id = str(movie["id"])
        cache[movie_id]["keywords"] = movie["keywords"]
        cache[movie_id]["cached_at"] = datetime.now().isoformat()

    movies = fetched_kw + fresh_kw

    save_cache(cache)
    print(f"Cache saved with {len(cache)} entries")

    filter_and_save(movies, genre_map.result())


if __name__ == "__main__":
    reel_collection_pipeline()