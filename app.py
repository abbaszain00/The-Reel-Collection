import streamlit as st 
import pandas as pd 

@st.cache_data
def load_data():
    return pd.read_csv("reel_collection.csv")
df = load_data()

st.title("🎬 The Reel Collection")

if "genre" not in st.session_state:
    st.session_state.genre = "All genres"

if "language" not in st.session_state:
    st.session_state.language = "All languages"



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
    df["original_language"].dropna().unique()
)

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

selected_filter = st.sidebar.selectbox(
    "Order by",
    list(sort_options.keys())
)

filtered_df = df.copy()

# Apply genre filter if selected
if selected_genre != "All genres":
    filtered_df = filtered_df[
        filtered_df["genres"].str.contains(selected_genre, na=False)]

# Apply language filter if selected
if selected_language != "All languages":
    filtered_df = filtered_df[
        filtered_df["original_language"] == selected_language]

#sort and take top 5
filtered_df = (
    filtered_df
    .sort_values(sort_options[selected_filter], ascending=False)
    .head(5)
)

if st.sidebar.button("Reset filters"):
    st.session_state.genre = "All genres"
    st.session_state.language = "All languages"

st.subheader(f"Top 5 {selected_genre} Movies")

for _, row in filtered_df.iterrows():
    col1, col2 = st.columns([1, 3])

    with col1:
        if pd.notna(row["poster_path"]):
            poster_url = f"https://image.tmdb.org/t/p/w500{row['poster_path']}"
            st.image(poster_url, use_container_width=True)

    with col2:
        st.markdown(f"### {row['title']}")
        st.write(f"⭐ {row['vote_average']}")
        st.write(f"**Year:** {row['year']}")
        st.write(f"**Genres:** {row['genres']}")