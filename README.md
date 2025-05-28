# Spotify Chart Tracker

An automated ETL pipeline that fetches and stores daily Spotify chart data, including artists, tracks, and genres, with a Reflex dashboard in development.

---

## 🔍 Overview

This project automates the ingestion and storage of Spotify playlist data into a PostgreSQL database using Prefect, Spotipy, and SQLAlchemy. It enriches this data with artist metadata and organizes it into a normalized schema for analysis and dashboarding.

- Solves the problem of continuously tracking and storing evolving chart data from Spotify.
- Ideal for music analysts, data engineers, or anyone interested in artist/track popularity trends.
- Built as a real-world ETL pipeline with ongoing dashboard development using Reflex (WIP).

---

## 🛠️ Tech Stack

- Python 3
- Prefect (ETL orchestration)  
- Spotipy (Spotify API)  
- SQLAlchemy / psycopg (Database interaction)  
- PostgreSQL  
- Pandas  
- Reflex (Dashboard frontend - WIP)  
- PrefectCloud
- dotenv (Secrets management)  

---

## 🚀 Features

- Automatically fetches and processes data from a Spotify playlist (e.g., Today's Top Hits)  
- Creates a normalized PostgreSQL schema to store:
  - Artists, Tracks, Genres, Chart Positions  
- Hash-checks to prevent redundant data ingestion  
- Fetches artist popularity, followers, and genres using Spotify’s metadata  
- Uses Prefect for task management, scheduling, and logging  
- Designed for daily cron-based execution (`exec_flow.serve(cron="0 0 * * *")`)  
- Frontend dashboard (in Reflex) under active development  

---

## 📁 Project Structure

```
/projectroot
│
├── .env                     # Environment variables
├── flows.py                 # Main ETL script (shown above)
├── /tests                   # Tests for final product
└── README.md                # This file
```

---

## 📈 Results

- Captures daily snapshot of chart metadata from Spotify  
- Enables genre-based analysis and popularity trends  
- Lays foundation for visualizations (e.g., most popular genres, artists over time)

> _Currently powering a Reflex dashboard (WIP) to monitor chart dynamics visually._

---

## 🧠 What I Learned

- How to orchestrate data pipelines with Prefect and modular task logic  
- Best practices for schema normalization and foreign key relationships in PostgreSQL  
- Using hashes to detect data changes and avoid redundant ingestion  
- Handling nested API structures like artist genre arrays  
- Designing pipelines with future dashboarding in mind  

---

## 📦 Installation & Usage

```bash
git clone https://github.com/NathanielH-snek/SpotifyChartTracker.git
cd SpotifyChartTracker
```
Setup a virtual environment if you'd like
```bash
pip install -r requirements.txt
touch .env
# Add DB_URI and ENGINE_URI to .env
python flows.py
```

> You can also deploy this with Prefect Cloud or run on a cron schedule via `exec_flow.serve(...)`
