# =========================================================
# COMBO DASHBOARD (ONE FILE)
# TruthSocial (Trump) + Polymarket (price_change)
# - Fixed schema based on your parquet columns
# - Truth: created_at, content
# - Poly: event_ts, market_id, price (+ computed returns)
# Includes:
# - Temporal trends (hour/day, week vs weekend, normalized)
# - Wordcloud by hour (Truth)
# - Sentiment VADER + LDA topics (Truth)
# - Polymarket movers/volatility (computed from price diffs)
# - Cross correlation + simple event-study
# - PDF export
# - Docker + uv snippet
# =========================================================

import os
import re
import io
from pathlib import Path
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient

import matplotlib.pyplot as plt
from wordcloud import WordCloud

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm

# -----------------------
# HARD CONFIG (LOCKED)
# -----------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "polymarket_db")

# TruthSocial schema (locked)
TRUTH_COLLECTION = "truthsocial_posts"
TRUTH_TIME_COL = "created_at"
TRUTH_TEXT_COL = "content"

# Polymarket schema (locked)
POLY_COLLECTION = "price_change"
POLY_TIME_COL = "event_ts"
POLY_MARKET_COL = "market_id"
POLY_PRICE_COL = "price"  # numeric after coercion

DEFAULT_LIMIT = 300_000

# Data base path (supports Docker mount at /app/data or local ./data)
DATA_BASE = Path(os.getenv("DATA_PATH", "/app/data"))
if not DATA_BASE.exists():
    DATA_BASE = Path("data")  # Fallback for local dev

# Optional parquet fallback (same structure as your repo)
PARQUET_TRUTH = DATA_BASE / "parquet/truthsocial/posts"
PARQUET_POLY = DATA_BASE / "parquet/polymarket/price_change"

# Optional market metadata mapping (JSON / collection)
# If you have a Mongo collection for "new_markets", set it here. Otherwise it will try parquet/json fallback.
POLY_MARKETS_COLLECTION = "new_markets"  # only used if exists; safe if not


# =======================
# Helpers
# =======================
def strip_html(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


@st.cache_data(ttl=60)  # Cache 60 secondes pour rafraîchir les données
def load_mongo(collection: str, limit: int) -> pd.DataFrame:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        col = client[DB_NAME][collection]
        df = pd.DataFrame(list(col.find({}, {"_id": 0}).limit(limit)))
        client.close()
        return df
    except Exception as e:
        st.warning(f"Erreur MongoDB: {e}")
        return pd.DataFrame()


def mongo_has_collection(collection: str) -> bool:
    """Vérifie si une collection existe dans MongoDB (sans cache pour toujours voir l'état actuel)."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        ok = collection in db.list_collection_names()
        client.close()
        return ok
    except Exception as e:
        st.warning(f"Connexion MongoDB échouée: {e}")
        return False


@st.cache_data
def load_all_parquet(base: Path) -> pd.DataFrame:
    """Load ALL parquet files from directory, not just the latest."""
    if not base.exists():
        return pd.DataFrame()
    files = sorted(base.glob("**/*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


@st.cache_data
def vader_sentiment(series: pd.Series) -> pd.Series:
    analyzer = SentimentIntensityAnalyzer()
    return series.astype(str).apply(lambda x: analyzer.polarity_scores(x)["compound"])


@st.cache_data
def lda_topics(texts: pd.Series, n_topics: int = 5, max_features: int = 5000) -> List[Tuple[int, str]]:
    s = texts.dropna().astype(str)
    if len(s) > 8000:
        s = s.sample(8000, random_state=42)

    vec = CountVectorizer(max_df=0.9, min_df=10, stop_words="english", max_features=max_features)
    X = vec.fit_transform(s)

    lda = LatentDirichletAllocation(n_components=n_topics, random_state=42, learning_method="online")
    lda.fit(X)

    words = vec.get_feature_names_out()
    topics = []
    for i, comp in enumerate(lda.components_):
        top_words = [words[j] for j in comp.argsort()[:-10:-1]]
        topics.append((i + 1, ", ".join(top_words)))
    return topics


def zscore(series: pd.Series) -> pd.Series:
    sd = series.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return series * 0.0
    return (series - series.mean()) / sd


def pdf_report(title: str, summary: List[str], tables: List[Tuple[str, pd.DataFrame]]) -> io.BytesIO:
    buf = io.BytesIO()
    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph(f"<b>{title}</b>", styles["Title"]))
    elements.append(Spacer(1, 0.7 * cm))

    elements.append(Paragraph("<b>Résumé</b>", styles["Heading2"]))
    for line in summary:
        elements.append(Paragraph(line, styles["BodyText"]))
    elements.append(PageBreak())

    for t, df in tables:
        elements.append(Paragraph(f"<b>{t}</b>", styles["Heading2"]))
        show = df.head(35).copy()
        elements.append(Paragraph(show.to_string(index=False), styles["Code"]))
        elements.append(Spacer(1, 0.6 * cm))

    doc = SimpleDocTemplate(buf, pagesize=A4)
    doc.build(elements)
    buf.seek(0)
    return buf


@st.cache_data
def load_market_metadata() -> pd.DataFrame:
    """
    Tries to get a mapping: market_id -> question/slug
    - Prefer Mongo collection new_markets if present
    - Else returns empty DF (app will fallback to market_id)
    """
    if mongo_has_collection(POLY_MARKETS_COLLECTION):
        m = load_mongo(POLY_MARKETS_COLLECTION, 200_000)
        # expected fields often include: id, question, slug, market (address), timestamp...
        # We normalize to market_id as string
        if not m.empty:
            if "id" in m.columns:
                m["market_id"] = m["id"].astype(str)
            elif "market_id" in m.columns:
                m["market_id"] = m["market_id"].astype(str)
            else:
                return pd.DataFrame()

            keep = [c for c in ["market_id", "question", "slug", "market"] if c in m.columns]
            m = m[keep].drop_duplicates("market_id")
            return m
    return pd.DataFrame()


# =======================
# App
# =======================
st.set_page_config(page_title="TruthSocial + Polymarket (price_change)", layout="wide")
st.title("🧩 Dashboard combiné — TruthSocial (Trump) + Polymarket (price_change)")

with st.sidebar:
    st.header("⚙️ Chargement")
    truth_source = st.radio("TruthSocial source", ["MongoDB", "Parquet"], index=0)
    poly_source = st.radio("Polymarket source", ["MongoDB", "Parquet"], index=0)

    limit_truth = st.number_input("Max docs TruthSocial (Mongo)", 10_000, 2_000_000, DEFAULT_LIMIT, 50_000)
    limit_poly = st.number_input("Max docs Polymarket (Mongo)", 10_000, 2_000_000, DEFAULT_LIMIT, 50_000)

    page = st.radio(
        "Navigation",
        [
            "🏠 Accueil",
            "🟦 TruthSocial — Activité",
            "🟦 TruthSocial — NLP",
            "🟧 Polymarket — Activité",
            "🟧 Polymarket — Markets/Volatilité",
            "🔗 Cross — Corrélations & Event study",
            "📄 Export PDF",
        ],
        index=0
    )

# ---- Load Truth
with st.spinner("Chargement TruthSocial..."):
    if truth_source == "MongoDB" and mongo_has_collection(TRUTH_COLLECTION):
        truth_raw = load_mongo(TRUTH_COLLECTION, int(limit_truth))
        truth_label = f"MongoDB · {DB_NAME}.{TRUTH_COLLECTION}"
    else:
        truth_raw = load_all_parquet(PARQUET_TRUTH)
        truth_label = f"Parquet · {PARQUET_TRUTH}"

# ---- Load Poly
with st.spinner("Chargement Polymarket (price_change)..."):
    if poly_source == "MongoDB" and mongo_has_collection(POLY_COLLECTION):
        poly_raw = load_mongo(POLY_COLLECTION, int(limit_poly))
        poly_label = f"MongoDB · {DB_NAME}.{POLY_COLLECTION}"
    else:
        poly_raw = load_all_parquet(PARQUET_POLY)
        poly_label = f"Parquet · {PARQUET_POLY}"

# ---- Show columns (debug)
with st.expander("🔍 Colonnes (verrouillées + debug)", expanded=False):
    st.write("TruthSocial cols:", list(truth_raw.columns))
    st.write("Polymarket cols:", list(poly_raw.columns))

# =======================
# Preprocess TruthSocial (locked)
# =======================
truth_df = pd.DataFrame()
if not truth_raw.empty:
    if TRUTH_TIME_COL not in truth_raw.columns or TRUTH_TEXT_COL not in truth_raw.columns:
        st.error(f"TruthSocial: colonnes manquantes attendues: {TRUTH_TIME_COL}, {TRUTH_TEXT_COL}")
        st.stop()

    truth_df = truth_raw.copy()
    truth_df[TRUTH_TIME_COL] = pd.to_datetime(truth_df[TRUTH_TIME_COL], errors="coerce", utc=True)
    truth_df = truth_df.dropna(subset=[TRUTH_TIME_COL, TRUTH_TEXT_COL])

    truth_df["clean_text"] = truth_df[TRUTH_TEXT_COL].map(strip_html)
    truth_df["hour"] = truth_df[TRUTH_TIME_COL].dt.hour
    truth_df["date"] = truth_df[TRUTH_TIME_COL].dt.date
    truth_df["weekday"] = truth_df[TRUTH_TIME_COL].dt.weekday
    truth_df["is_weekend"] = truth_df["weekday"] >= 5

    truth_df["sentiment"] = vader_sentiment(truth_df["clean_text"])

# =======================
# Preprocess Polymarket (locked)
# =======================
poly_df = pd.DataFrame()
if not poly_raw.empty:
    missing = [c for c in [POLY_TIME_COL, POLY_MARKET_COL, POLY_PRICE_COL] if c not in poly_raw.columns]
    if missing:
        st.error(f"Polymarket: colonnes manquantes attendues: {missing}")
        st.stop()

    poly_df = poly_raw.copy()
    poly_df[POLY_TIME_COL] = pd.to_datetime(poly_df[POLY_TIME_COL], errors="coerce", utc=True)
    poly_df = poly_df.dropna(subset=[POLY_TIME_COL, POLY_MARKET_COL, POLY_PRICE_COL])

    poly_df[POLY_MARKET_COL] = poly_df[POLY_MARKET_COL].astype(str)
    poly_df[POLY_PRICE_COL] = pd.to_numeric(poly_df[POLY_PRICE_COL], errors="coerce")
    poly_df = poly_df.dropna(subset=[POLY_PRICE_COL])

    poly_df["hour"] = poly_df[POLY_TIME_COL].dt.hour
    poly_df["date"] = poly_df[POLY_TIME_COL].dt.date
    poly_df["weekday"] = poly_df[POLY_TIME_COL].dt.weekday
    poly_df["is_weekend"] = poly_df["weekday"] >= 5

    # compute per-market returns (diff) using event stream
    poly_df = poly_df.sort_values([POLY_MARKET_COL, POLY_TIME_COL])
    poly_df["price_diff"] = poly_df.groupby(POLY_MARKET_COL)[POLY_PRICE_COL].diff()
    poly_df["abs_diff"] = poly_df["price_diff"].abs()

# optional market metadata
market_meta = load_market_metadata()
if not market_meta.empty and not poly_df.empty:
    poly_df = poly_df.merge(market_meta, on="market_id", how="left")

def market_label(row) -> str:
    if "question" in row and isinstance(row["question"], str) and row["question"].strip():
        return row["question"]
    if "slug" in row and isinstance(row["slug"], str) and row["slug"].strip():
        return row["slug"]
    return str(row.get("market_id", "UNKNOWN"))

if not poly_df.empty:
    poly_df["_market_label"] = poly_df.apply(market_label, axis=1)
else:
    poly_df["_market_label"] = pd.Series(dtype=str)

# =======================
# HOME
# =======================
if page == "🏠 Accueil":
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🟦 TruthSocial")
        st.caption(truth_label)
        if truth_df.empty:
            st.warning("TruthSocial vide / non chargé.")
        else:
            st.metric("Posts", f"{len(truth_df):,}".replace(",", " "))
            st.metric("Période", f"{truth_df['date'].min()} → {truth_df['date'].max()}")
            st.caption(f"Locked cols: time={TRUTH_TIME_COL}, text={TRUTH_TEXT_COL}")

    with c2:
        st.subheader("🟧 Polymarket (price_change)")
        st.caption(poly_label)
        if poly_df.empty:
            st.warning("Polymarket vide / non chargé.")
        else:
            st.metric("Events", f"{len(poly_df):,}".replace(",", " "))
            st.metric("Période", f"{poly_df['date'].min()} → {poly_df['date'].max()}")
            st.caption(f"Locked cols: time={POLY_TIME_COL}, market={POLY_MARKET_COL}, price={POLY_PRICE_COL}")

# =======================
# Truth activity
# =======================
elif page == "🟦 TruthSocial — Activité":
    if truth_df.empty:
        st.error("TruthSocial vide.")
        st.stop()

    st.subheader("⏰ Tendance horaire")
    hourly = truth_df.groupby("hour").size().reset_index(name="posts")
    st.bar_chart(hourly, x="hour", y="posts", use_container_width=True)

    st.subheader("📊 Moyenne par jour vs heure")
    dh = truth_df.groupby(["date", "hour"]).size().reset_index(name="posts")
    avg = dh.groupby("hour")["posts"].mean().reset_index()
    st.line_chart(avg, x="hour", y="posts", use_container_width=True)

    st.subheader("📅 Semaine vs week-end")
    wk = truth_df.groupby(["is_weekend", "hour"]).size().reset_index(name="posts")
    wk["type"] = wk["is_weekend"].map({False: "Semaine", True: "Week-end"})
    st.line_chart(wk, x="hour", y="posts", color="type", use_container_width=True)

    st.subheader("📈 Tendance normalisée (posts/jour)")
    daily = truth_df.groupby("date").size().reset_index(name="posts")
    daily["normalized"] = zscore(daily["posts"])
    st.line_chart(daily, x="date", y="normalized", use_container_width=True)

    st.subheader("🎯 Heures stratégiques (pics volume)")
    hourly["z"] = zscore(hourly["posts"])
    strategic = hourly.reindex(hourly["z"].abs().sort_values(ascending=False).index).head(5)
    st.dataframe(strategic, use_container_width=True)

# =======================
# Truth NLP
# =======================
elif page == "🟦 TruthSocial — NLP":
    if truth_df.empty:
        st.error("TruthSocial vide.")
        st.stop()

    st.subheader("🧠 Sentiment (VADER)")
    sent_daily = truth_df.groupby("date")["sentiment"].mean().reset_index()
    st.line_chart(sent_daily, x="date", y="sentiment", use_container_width=True)

    st.subheader("🔥 Nuage de mots par heure")
    hour = st.slider("Heure", 0, 23, 9)
    text = " ".join(truth_df[truth_df["hour"] == hour]["clean_text"].astype(str).tolist())
    if len(text) < 100:
        st.warning("Pas assez de texte pour ce créneau.")
    else:
        wc = WordCloud(width=1400, height=700, background_color="white", collocations=False).generate(text)
        fig, ax = plt.subplots(figsize=(14, 7))
        ax.imshow(wc)
        ax.axis("off")
        st.pyplot(fig)

    st.subheader("🧩 Topics (LDA)")
    n_topics = st.slider("Nombre de topics", 3, 12, 5)
    if st.button("Calculer topics"):
        topics = lda_topics(truth_df["clean_text"], n_topics=n_topics)
        for i, words in topics:
            st.write(f"**Topic {i}** → {words}")

# =======================
# Poly activity
# =======================
elif page == "🟧 Polymarket — Activité":
    if poly_df.empty:
        st.error("Polymarket vide.")
        st.stop()

    st.subheader("⏰ Activité par heure (events)")
    hourly = poly_df.groupby("hour").size().reset_index(name="events")
    st.bar_chart(hourly, x="hour", y="events", use_container_width=True)

    st.subheader("📅 Semaine vs week-end")
    wk = poly_df.groupby(["is_weekend", "hour"]).size().reset_index(name="events")
    wk["type"] = wk["is_weekend"].map({False: "Semaine", True: "Week-end"})
    st.line_chart(wk, x="hour", y="events", color="type", use_container_width=True)

    st.subheader("📈 Activité journalière normalisée")
    daily = poly_df.groupby("date").size().reset_index(name="events")
    daily["normalized"] = zscore(daily["events"])
    st.line_chart(daily, x="date", y="normalized", use_container_width=True)

    st.subheader("🎯 Heures stratégiques (pics volume)")
    hourly["z"] = zscore(hourly["events"])
    strategic = hourly.reindex(hourly["z"].abs().sort_values(ascending=False).index).head(5)
    st.dataframe(strategic, use_container_width=True)

# =======================
# Poly markets & volatility
# =======================
elif page == "🟧 Polymarket — Markets/Volatilité":
    if poly_df.empty:
        st.error("Polymarket vide.")
        st.stop()

    st.subheader("🏆 Top markets (activité)")
    top_n = st.slider("Top N", 5, 50, 15)

    top_markets = (
        poly_df.groupby("_market_label")
              .size()
              .reset_index(name="events")
              .sort_values("events", ascending=False)
              .head(top_n)
    )
    st.dataframe(top_markets, use_container_width=True)

    st.subheader("🚀 Movers (sur variations de prix calculées)")
    # Use abs_diff over period as a robust mover proxy
    movers = (
        poly_df.dropna(subset=["price_diff"])
              .groupby("_market_label")["price_diff"]
              .agg(avg_diff="mean", std="std", max_abs=lambda x: float(np.nanmax(np.abs(x))), count="count")
              .reset_index()
              .sort_values("max_abs", ascending=False)
              .head(top_n)
    )
    st.dataframe(movers, use_container_width=True)
    st.caption("Movers calculés via diff successive sur le flux price_change (pas besoin d’une colonne price_change).")

    st.subheader("🌪️ Distribution des variations (échantillon)")
    s = poly_df["price_diff"].dropna()
    if len(s) > 0:
        s = s.sample(min(20000, len(s)), random_state=42)
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.hist(s, bins=60)
        ax.set_xlabel("price_diff")
        ax.set_ylabel("count")
        st.pyplot(fig)
    else:
        st.warning("Pas assez de diffs calculables.")

# =======================
# Cross analysis
# =======================
elif page == "🔗 Cross — Corrélations & Event study":
    if truth_df.empty or poly_df.empty:
        st.error("Il faut TruthSocial ET Polymarket chargés.")
        st.stop()

    shared_min = max(truth_df["date"].min(), poly_df["date"].min())
    shared_max = min(truth_df["date"].max(), poly_df["date"].max())
    if shared_min > shared_max:
        st.error("Pas de période commune.")
        st.stop()

    st.subheader("📌 Période commune")
    st.write(f"{shared_min} → {shared_max}")

    t = truth_df[(truth_df["date"] >= shared_min) & (truth_df["date"] <= shared_max)]
    p = poly_df[(poly_df["date"] >= shared_min) & (poly_df["date"] <= shared_max)]

    truth_daily = t.groupby("date").agg(posts=("date", "size"), sentiment=("sentiment", "mean")).reset_index()
    poly_daily = p.groupby("date").agg(
        pm_events=("date", "size"),
        pm_abs_move=("abs_diff", "mean")
    ).reset_index()

    merged = pd.merge(truth_daily, poly_daily, on="date", how="inner").sort_values("date")

    st.subheader("🔗 Corrélations (journalières)")
    if merged.shape[0] < 5:
        st.warning("Pas assez de jours communs.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Corr(posts, pm_events)", f"{merged['posts'].corr(merged['pm_events']):.3f}")
        c2.metric("Corr(posts, pm_abs_move)", f"{merged['posts'].corr(merged['pm_abs_move']):.3f}")
        c3.metric("Corr(sentiment, pm_abs_move)", f"{merged['sentiment'].corr(merged['pm_abs_move']):.3f}")

        st.line_chart(merged.set_index("date")[["posts", "pm_events"]], use_container_width=True)
        st.line_chart(merged.set_index("date")[["pm_abs_move"]], use_container_width=True)

    st.subheader("🧪 Event study : pm_abs_move autour des jours “rafales” (posts)")
    q = st.slider("Seuil quantile (posts)", 0.80, 0.99, 0.90, 0.01)
    window = st.slider("Fenêtre (jours avant/après)", 1, 10, 3)

    threshold = merged["posts"].quantile(q)
    event_days = merged[merged["posts"] >= threshold]["date"].tolist()

    if len(event_days) == 0:
        st.info("Aucun event day au seuil choisi.")
    else:
        idx = merged.set_index("date")
        rel_rows = []
        for d in event_days:
            for k in range(-window, window + 1):
                day = (pd.to_datetime(d) + pd.Timedelta(days=k)).date()
                if day in idx.index:
                    rel_rows.append({"rel_day": k, "pm_abs_move": float(idx.loc[day, "pm_abs_move"])})
        rel = pd.DataFrame(rel_rows)
        if rel.empty:
            st.info("Pas assez de points autour des event days.")
        else:
            rel_mean = rel.groupby("rel_day")["pm_abs_move"].mean().reset_index()
            st.line_chart(rel_mean, x="rel_day", y="pm_abs_move", use_container_width=True)
            st.dataframe(rel_mean, use_container_width=True)

# =======================
# PDF Export
# =======================
elif page == "📄 Export PDF":
    st.subheader("📄 Export PDF")

    summary = [
        f"Truth source: {truth_label}",
        f"Poly source: {poly_label}",
        f"Truth posts: {len(truth_df):,}".replace(",", " ") if not truth_df.empty else "Truth vide",
        f"Poly events: {len(poly_df):,}".replace(",", " ") if not poly_df.empty else "Poly vide",
        f"Truth schema: time={TRUTH_TIME_COL}, text={TRUTH_TEXT_COL}",
        f"Poly schema: time={POLY_TIME_COL}, market={POLY_MARKET_COL}, price={POLY_PRICE_COL}",
    ]

    tables = []
    if not truth_df.empty:
        tables.append(("Truth — posts par heure", truth_df.groupby("hour").size().reset_index(name="posts")))
        tables.append(("Truth — posts par jour (last 60)", truth_df.groupby("date").size().reset_index(name="posts").tail(60)))

    if not poly_df.empty:
        tables.append(("Poly — events par heure", poly_df.groupby("hour").size().reset_index(name="events")))
        tables.append(("Poly — top markets", poly_df.groupby("_market_label").size().reset_index(name="events").sort_values("events", ascending=False).head(20)))

    # cross corr
    if not truth_df.empty and not poly_df.empty:
        shared_min = max(truth_df["date"].min(), poly_df["date"].min())
        shared_max = min(truth_df["date"].max(), poly_df["date"].max())
        if shared_min <= shared_max:
            t = truth_df[(truth_df["date"] >= shared_min) & (truth_df["date"] <= shared_max)]
            p = poly_df[(poly_df["date"] >= shared_min) & (poly_df["date"] <= shared_max)]
            truth_daily = t.groupby("date").agg(posts=("date", "size"), sentiment=("sentiment", "mean")).reset_index()
            poly_daily = p.groupby("date").agg(pm_events=("date", "size"), pm_abs_move=("abs_diff", "mean")).reset_index()
            merged = pd.merge(truth_daily, poly_daily, on="date", how="inner").sort_values("date")
            if merged.shape[0] >= 5:
                summary.append(f"Corr(posts, pm_events): {merged['posts'].corr(merged['pm_events']):.3f}")
                summary.append(f"Corr(posts, pm_abs_move): {merged['posts'].corr(merged['pm_abs_move']):.3f}")
                tables.append(("Cross — daily merged (last 60)", merged.tail(60)))

    pdf = pdf_report("TruthSocial + Polymarket — Report", summary, tables or [("Info", pd.DataFrame({"note": ["No tables"]}))])

    st.download_button(
        "📥 Télécharger le PDF",
        data=pdf,
        file_name="truthsocial_polymarket_report.pdf",
        mime="application/pdf"
    )

st.markdown("---")
st.caption("MongoDB · Streamlit · VADER · LDA · PDF · uv · schema locked (Truth + price_change)")

#with st.expander("🐳 Docker + uv (cloud-ready)"):
#    st.write("""
#**Install (uv):**
#```bash
#uv pip install streamlit pandas pymongo python-dotenv matplotlib wordcloud vaderSentiment scikit-learn reportlab pyarrow
