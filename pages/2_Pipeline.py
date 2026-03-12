import streamlit as st

st.set_page_config(layout="wide", page_title="The Pipeline", page_icon="⚙️")

st.title("⚙️ The Pipeline")
st.write("Built with Prefect. Pulls live data from TMDB, enriches it, filters it, and saves it to a CSV that feeds the shelf.")

col_img, col_notes = st.columns([1.2, 1])

with col_img:
    st.image("final_pipeline_diagram.png", use_container_width=True)

with col_notes:
    st.markdown("#### Stages")
    st.markdown("""
- **Fetch Movies** — 200 films, 10 pages
- **Fetch Genre Map** — runs concurrently, maps IDs to names
- **Fetch Movie Details** — runtime, director, UK streaming in one request
- **Fetch Keywords** — top 5 per film, powers Theme filter
- **Filter & Save** — rating ≥7.0, votes ≥500, runtime ≥40min, not on major platforms
""")

    st.markdown("#### Design decisions")
    st.markdown("""
- **ThreadPoolTaskRunner** — concurrent API calls, ~30s total
- **`append_to_response`** — halves API calls per film
- **UK flatrate filter** — Netflix, Prime, Disney+, Apple TV+
- **Reusable** — re-runs on demand, schedulable via Prefect
""")