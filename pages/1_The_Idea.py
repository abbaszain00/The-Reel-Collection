import streamlit as st

st.set_page_config(layout="wide", page_title="The Idea", page_icon="💡")

st.title("💡 The Idea")

st.markdown("""
    <style>
        .metric-card {
            background-color: #1e1e1e;
            border-radius: 10px;
            padding: 1.5rem;
            text-align: center;
        }
        .metric-value {
            font-size: 2.2rem;
            font-weight: bold;
            color: #59C9A5;
            display: block;
        }
        .metric-label {
            font-size: 0.95rem;
            color: #aaa;
        }
        .section-header {
            color: #59C9A5;
            font-size: 1.3rem;
            font-weight: bold;
            margin-bottom: 0.5rem;
        }
    </style>
""", unsafe_allow_html=True)

# The problem
st.markdown("### The Streaming Paradox")
st.write("Streaming platforms push what is popular, not what is good. That means the highest-rated films are often the ones nobody can find.")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
        <div class="metric-card">
            <span class="metric-value">0.28</span>
            <span class="metric-label">Correlation between popularity and quality. Barely related.</span>
        </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
        <div class="metric-card">
            <span class="metric-value">Pre-2000s</span>
            <span class="metric-label">Older films rated consistently higher. Less marketing noise, more honest scores.</span>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
        <div class="metric-card">
            <span class="metric-value">History · War · Doc</span>
            <span class="metric-label">Highest rated genres on TMDB, but the least engaged with on streaming.</span>
        </div>
    """, unsafe_allow_html=True)

st.divider()

# The solution
st.markdown("### The Reel Collection")
st.write("A dedicated in-store shelf of films you cannot find on streaming. Curated using real audience ratings, not marketing budgets, and refreshed automatically using live TMDB data.")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown('<p class="section-header">What it is</p>', unsafe_allow_html=True)
    st.write("A physical shelf stocked with high-rated films absent from Netflix, Prime, Disney+ and Apple TV+. Updated monthly with themed collections.")

with col2:
    st.markdown('<p class="section-header">Who it\'s for</p>', unsafe_allow_html=True)
    st.write("Curators, Generational Nostalgists, and Collectionists. Customers who want something worth watching, not just something new.")

with col3:
    st.markdown('<p class="section-header">Why it works</p>', unsafe_allow_html=True)
    st.write("Moviever becomes the home of quality films streaming has forgotten. A clear brand identity, an untapped market, and a proprietary ratings dataset that grows over time.")