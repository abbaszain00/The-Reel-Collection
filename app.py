import streamlit as st 
import pandas as pd 
from datetime import datetime

st.set_page_config(layout="wide", page_title="The Reel Collection", page_icon="🎬")

@st.cache_data
def load_data():
    return pd.read_csv("reel_collection.csv")
df = load_data()

st.title("🎬 The Reel Collection")

st.markdown("""
    <style>
        .stat-bar { 
            display: flex; 
            gap: 2rem; 
            padding: 1rem;  
            border-radius: 8px; 
            margin-bottom: 1.5rem;
        }
        .stat-item { 
            color: white; 
            font-size: 1rem; 
            text-align: center;
        }
        .stat-value { 
            font-size: 1.6rem; 
            font-weight: bold; 
            color: #59C9A5; 
            display: block;
        }
        /* Smaller director text */
        .director-text {
            font-size: 0.7rem;
            color: #888;
            margin-top: 0.5rem;
            margin-bottom: 0.5rem;
        } 
    </style>
""", unsafe_allow_html=True)

if "genre" not in st.session_state:
    st.session_state.genre = "All genres"

if "language" not in st.session_state:
    st.session_state.language = "All languages"

if "years" not in st.session_state:
    st.session_state.years = (1900, 2025)

if "ascending" not in st.session_state:
    st.session_state.ascending = False
if "keywords" not in st.session_state:
    st.session_state.keywords = []

# Build genre list
all_genres = (
    df["genres"]
    .str.split(", ")
    .explode()
    .dropna()
    .unique()
)

# Build language list
all_languages = (
    df["language"].dropna().unique()
)

# Build director list
all_directors = (
    df["director"]
    .str.split(", ")
    .explode()
    .dropna()
    .unique()
)

# Reset filters
if st.sidebar.button("🔄 Reset filters"):
    st.session_state.genre = "All genres"
    st.session_state.language = "All languages"
    st.session_state.director = "All directors"
    st.session_state.years = (1900, 2025)
    st.session_state.keywords = []
    st.rerun()

# Refresh shelf
if st.sidebar.button("🔄 Refresh shelf"):
    import subprocess
    import sys
    with st.spinner("Running pipeline..."):
        result = subprocess.run(
            [sys.executable, "pipeline.py"],
            capture_output=True,
            text=True
        )
        st.write(result.stdout)
        st.write(result.stderr)
    st.cache_data.clear()
    st.rerun()

selected_genre = st.sidebar.selectbox(
    "Choose a genre",
    ["All genres"] + sorted(all_genres.tolist()),
    key="genre"
)

selected_language = st.sidebar.selectbox(
    "Choose a Language",
    ["All languages"] + sorted(all_languages.tolist()),
    key="language"
)

selected_director = st.sidebar.selectbox(
    "Director",
    ["All directors"] + sorted(all_directors.tolist()),
    key="director"
)
st.sidebar.divider()

# Build a list to sort results by
sort_options = {
    "Vote Average ⭐": "vote_average",
    "Year 📅": "year",
    "Vote Count 🗳️": "vote_count"
}

def toggle_sort():
    st.session_state.ascending = not st.session_state.ascending

col1, col2 = st.sidebar.columns([4,1])

with col1:
    selected_filter = st.selectbox(
        "Order by",
        list(sort_options.keys())
    )
with col2:
    arrow = "⬆️" if st.session_state.ascending else "⬇️"
    st.button(arrow, on_click=toggle_sort, key="sort_direction")


# Year filter slider - placed before genre filtering
years = st.sidebar.slider("Year Range", 1900, 2025, key="years")


filtered_df = df.copy()

# Build keyword list
all_keywords = (
    df["keywords"]
    .dropna()
    .str.split(", ")
    .explode()
    .str.strip()
)
keyword_counts = all_keywords.value_counts()

EXCLUDED_KEYWORDS = {"aftercreditsstinger", "duringcreditsstinger"}

common_keywords = sorted(
    k for k, count in keyword_counts.items()
    if count >= 3 and k not in EXCLUDED_KEYWORDS
)
selected_keywords = st.sidebar.multiselect("Theme", common_keywords, key="keywords")

# Apply genre filter if selected
if selected_genre != "All genres":
    filtered_df = filtered_df[
        filtered_df["genres"].str.contains(selected_genre, na=False)]

# Apply language filter if selected
if selected_language != "All languages":
    filtered_df = filtered_df[
        filtered_df["language"] == selected_language]

# Apply a year filter
filtered_df = filtered_df[
    (filtered_df["year"] >= years[0]) &
    (filtered_df["year"] <= years[1])]

if selected_director != "All directors":
    filtered_df = filtered_df[
    filtered_df["director"].str.contains(selected_director, na=False)]

# Apply keyword filter
if selected_keywords:
    filtered_df = filtered_df[
        filtered_df["keywords"].apply(
            lambda x: any(k in x.split(", ") for k in selected_keywords) if pd.notna(x) else False
        )
    ]

#sort and take top 5
filtered_df = (
    filtered_df
    .sort_values(sort_options[selected_filter], ascending=st.session_state.ascending)
    .head(6)
)

# Debug info to show how many movies match the filters
total_matches = len(df.copy())

temp_df = df.copy()

if selected_genre != "All genres":
    temp_df = temp_df[temp_df["genres"].str.contains(selected_genre, na=False)]

if selected_language != "All languages":
    temp_df = temp_df[temp_df["language"] == selected_language]

if selected_keywords:
    temp_df = temp_df[
        temp_df["keywords"].apply(
            lambda x: any(k in x.split(", ") for k in selected_keywords) if pd.notna(x) else False
        )
    ]

if selected_director != "All directors":
    temp_df = temp_df[temp_df["director"].str.contains(selected_director, na=False)]

temp_df = temp_df[
    (temp_df["year"] >= years[0]) &
    (temp_df["year"] <= years[1])
]

total_matches = len(temp_df)

avg_rating = round(temp_df["vote_average"].mean(), 2) if not temp_df.empty else "N/A"
num_languages = temp_df["language"].nunique()
last_updated = df["last_updated"].iloc[0] if "last_updated" in df.columns else "N/A"
last_updated_raw = df["last_updated"].iloc[0] if "last_updated" in df.columns else None

if last_updated_raw:
    last_updated_dt = datetime.strptime(last_updated_raw, "%Y-%m-%d %H:%M:%S")
    diff = datetime.now() - last_updated_dt
    seconds = int(diff.total_seconds())
    if seconds < 60:
        last_updated = f"{seconds} seconds ago"
    elif seconds < 3600:
        last_updated = f"{seconds // 60} minutes ago"
    elif seconds < 86400:
        last_updated = f"{seconds // 3600} hours ago"
    else:
        last_updated = f"{seconds // 86400} days ago"
else:
    last_updated = "N/A"

st.markdown(f"""
    <div class="stat-bar">
        <div class="stat-item"><span class="stat-value">{len(temp_df)}</span>Films on the shelf</div>
        <div class="stat-item"><span class="stat-value">{num_languages}</span>Languages</div>
        <div class="stat-item"><span class="stat-value">⭐ {avg_rating}</span>Avg rating</div>
        <div class="stat-item"><span class="stat-value">{last_updated}</span>Last updated</div>
    </div>
""", unsafe_allow_html=True)

# Display movies in a 3x2 grid (3 in first row, 3 in second row)
movies_list = list(filtered_df.iterrows())
# First row - 3 movies
if len(movies_list) > 0:
    cols_row1 = st.columns(3)
    for idx in range(min(3, len(movies_list))):
        _, row = movies_list[idx]
        with cols_row1[idx]:
            if pd.notna(row["poster_path"]):
                poster_url = f"https://image.tmdb.org/t/p/w780{row['poster_path']}"
                st.image(poster_url, use_container_width=True)
            
            # Fixed height container for title
            st.markdown(f"""
                <div style="height: 60px; display: flex; align-items: center;">
                    <strong>{row['title']}</strong>
                </div>
            """, unsafe_allow_html=True)
            
            st.write(f"⭐ Rating: {row['vote_average']}/10")
            st.write(f"📅 Year: {row['year']}")
            st.write(f"🗳️ {row['vote_count']:,} votes")
            st.write(f"{row['genres']}")

            with st.expander("Show more details"):
                st.markdown(f'<p class="director-text">Directed by {row["director"]}</p>', unsafe_allow_html=True)
                st.write(f"**Overview:** {row['overview']}")

# Second row - remaining movies (up to 3 more)
if len(movies_list) > 3:
    cols_row2 = st.columns(3)
    for idx in range(3, min(6, len(movies_list))):
        _, row = movies_list[idx]
        with cols_row2[idx - 3]:
            if pd.notna(row["poster_path"]):
                poster_url = f"https://image.tmdb.org/t/p/w780{row['poster_path']}"
                st.image(poster_url, use_container_width=True)
            
            # Fixed height container for title
            st.markdown(f"""
                <div style="height: 60px; display: flex; align-items: center;">
                    <strong>{row['title']}</strong>
                </div>
            """, unsafe_allow_html=True)
            
            st.write(f"⭐ Rating: {row['vote_average']}/10")
            st.write(f"📅 Year: {row['year']}")
            st.write(f"🗳️ {row['vote_count']:,} votes")
            st.write(f"{row['genres']}")

            with st.expander("Show more details"):
                st.markdown(f'<p class="director-text">Directed by {row["director"]}</p>', unsafe_allow_html=True)
                st.write(f"**Overview:** {row['overview']}")

if len(movies_list) == 0:
    st.info("No movies found for this selection.")