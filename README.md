# MUSIC ETL Project

An ETL project to track the changes of the billboard top 100 via the official billboard top 100 playlist

## Pipeline
SpotifyAPI via Spotipy -> Pandas -> Postgresql -> Reflex(FastAPI + React)

All of this is facilitated via prefect which is scheduled to run once everyday

The frontend involving reflex is a WIP and will be a nice dashboard to track trends and some stats, as such it requires a bit more thought as to design. 