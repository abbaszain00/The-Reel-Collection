import streamlit as st 
import pandas as pd 

@st.cache_data
def load_data():
    return pd.read_csv("reel_collection.csv")
df = load_data()

st.title("🎬 The Reel Collection")

# Build genre list
all_genres = (
    df["genres"]
    .str.split(", ")
    .explode()
    .dropna()
    .unique()
)

selected_genre = st.sidebar.selectbox(
    "Choose a genre",
    sorted(all_genres)
)

# Filter movies containing that genre
filtered_df = (
    df[df["genres"].str.contains(selected_genre)]
    .sort_values("vote_average", ascending=False)
    .head(5)
)

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