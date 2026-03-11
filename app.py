import streamlit as st 
import pandas as pd 

st.set_page_config(layout="wide", page_title="The Reel Collection", page_icon="🎬")

@st.cache_data
def load_data():
    return pd.read_csv("reel_collection.csv")
df = load_data()

st.title("🎬 The Reel Collection")

if "genre" not in st.session_state:
    st.session_state.genre = "All genres"

if "language" not in st.session_state:
    st.session_state.language = "All languages"

if "years" not in st.session_state:
    st.session_state.years = (1900, 2025)

if "ascending" not in st.session_state:
    st.session_state.ascending = False

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

# Reset filters
if st.sidebar.button("🔄 Reset filters"):
    st.session_state.genre = "All genres"
    st.session_state.language = "All languages"
    st.session_state.years = (1900, 2025)
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
    (filtered_df["year"] <= years[1])
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

temp_df = temp_df[
    (temp_df["year"] >= years[0]) &
    (temp_df["year"] <= years[1])
]

total_matches = len(temp_df)

st.caption(
    f"Found {total_matches} movies in year range {years[0]}-{years[1]}, showing top {len(filtered_df)}"
)

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
            
            if pd.notna(row.get("vote_count")):
                st.caption(f"🗳️ {row['vote_count']:,} votes")
            
            st.markdown(f"""
                <div style="height: 60px; display: flex; align-items: center;">
                    {row['genres']}
                </div>
            """, unsafe_allow_html=True)
            #st.caption(f"Genres: {row['genres']}")
            with st.expander("Show more details"):
                if pd.notna(row.get("overview")):
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
            
            if pd.notna(row.get("vote_count")):
                st.caption(f"🗳️ {row['vote_count']:,} votes")
            
            st.markdown(f"""
                <div style="height: 60px; display: flex; align-items: center;">
                    {row['genres']}
                </div>
            """, unsafe_allow_html=True)
            #st.caption(f"Genres: {row['genres']}")
            with st.expander("Show more details"):
                if pd.notna(row.get("overview")):
                    st.write(f"**Overview:** {row['overview']}")
if len(movies_list) == 0:
    st.info("No movies found for this selection.")