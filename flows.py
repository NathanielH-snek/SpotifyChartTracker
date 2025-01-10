import hashlib
import os
from datetime import datetime

import pandas as pd
import psycopg
import spotipy
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger
from spotipy.oauth2 import SpotifyClientCredentials
from sqlalchemy import create_engine

load_dotenv()
authManager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=authManager)
playlistURL = "https://open.spotify.com/playlist/6UeSakyzhiEt4NB3UAd6NQ"
DB_CREDS = os.getenv("DB_URI")
ENGINE_CREDS = os.getenv("ENGINE_URI")
engine = create_engine(ENGINE_CREDS)

@task(name="Init")
def create_table():
    conn = psycopg.connect(conninfo=DB_CREDS)
    cursor = conn.cursor()
    #cursor.execute("PRAGMA foreign_keys = ON;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artists(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            followers INTEGER NOT NULL,
            popularity INTEGER NOT NULL
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracks(
            id TEXT PRIMARY KEY,
            track_name TEXT,
            "explicit" INTEGER,
            primary_artist_id TEXT NOT NULL,
            image TEXT,
            duration_sec INTEGER,
            FOREIGN KEY(primary_artist_id) REFERENCES artists(id)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chart_positions(
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            position INTEGER NOT NULL,
            track_id TEXT NOT NULL,
            FOREIGN KEY(track_id) REFERENCES tracks(id)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genres(
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS artist_genres(
            id SERIAL PRIMARY KEY,
            artist_id TEXT NOT NULL,
            genre_name TEXT NOT NULL,
            FOREIGN KEY(artist_id) REFERENCES artists(id),
            UNIQUE (artist_id, genre_name)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS track_artists(
            id SERIAL PRIMARY KEY,
            track_id TEXT NOT NULL,
            artist_id TEXT NOT NULL,
            FOREIGN KEY(track_id) REFERENCES tracks(id),
            FOREIGN KEY(artist_id) REFERENCES artists(id)
        );
    """)

    conn.commit()
    conn.close()


@task(name="Get Spotify Data")
def get_tracks():
    today = datetime.utcnow().date()
    data = sp.playlist_tracks(playlistURL)
    tracks = []
    for position, item in enumerate(data["items"]):
        track = item["track"]
        artists = item["track"]["album"]["artists"]
        artistIds = [x["id"] for x in artists]
        cleanedDict = {
            # Boolean
            "explicit": int(track["explicit"]),
            # Album Image URL
            "image": track["album"]["images"][0]["url"],
            # Album Title
            "album_title": track["album"]["name"],
            # Album Release Date
            "track_release": track["album"]["release_date"],
            # List of Artist Ids
            # Removed for now since no tangible benefit
            #"artists": artistIds,
            # First Artist
            "primary_artist_id": artistIds[0],
            "track_name": track["name"],
            # Duration converted to seconds
            "duration_sec": track["duration_ms"] / 1000,
            "id": track["id"],
            #'track_popularity': track['popularity'],
            "position": position,
            # Data accessed
            "date": today,
        }
        tracks.append(cleanedDict)
    df = pd.DataFrame(tracks)
    return df


@task(name="Verify Data New")
def check_data(df):
    conn = psycopg.connect(conninfo=DB_CREDS)
    cursor = conn.cursor()
    # Pass df created by getting tracks
    # df['artists'].apply(lambda x: ', '.join(x))
    # print((df.map(type) == list).all())
    newHash = int(
        hashlib.sha256(
            pd.util.hash_pandas_object(df, index=True).values
        ).hexdigest(),
        16,
    )
    cursor.execute("SELECT EXISTS(select * from information_schema.tables where table_name=%s)", ('requests',))
    if cursor.fetchone()[0]:
        cursor.execute("SELECT * FROM requests ORDER BY date DESC LIMIT 1")
        recentRow = cursor.fetchone()
        if not recentRow:
            conn.commit()
            conn.close()
            return True
        else:
            oldHash = recentRow[1]
            if int(oldHash) == newHash:
                logger = get_run_logger()
                logger.info("Data is not new")
                conn.commit()
                conn.close()
                return False
            else:
                conn.commit()
                conn.close()
                return True
    else:
        cursor.execute("""
               CREATE TABLE IF NOT EXISTS requests(
                    id SERIAL PRIMARY KEY,
                    req_hash TEXT NOT NULL,
                    date DATE NOT NULL
                );
            """)
        conn.commit()
        conn.close()
        return True


@task(name="Save Fetched Data")
def save_hash(df):
    conn = psycopg.connect(conninfo=DB_CREDS)
    newHash = int(
            hashlib.sha256(
                pd.util.hash_pandas_object(df, index=True).values
            ).hexdigest(),
            16,
    )
    reqdf = pd.DataFrame({
        "req_hash": [str(newHash)],
        "date": [datetime.utcnow().date()]
    })
    reqdf.to_sql("requests", engine, if_exists="append", index=False)
    conn.commit()
    conn.close()

@task(name="Fetch Artist Data")
def get_artist_data(newData, df):
    if newData:
        # Tracks df once more
        cleanDict = []
        for val in df["primary_artist_id"]:
            data = sp.artist(val)
            clean = {
                "id": val,
                "name": data["name"],
                "followers": data["followers"]["total"],
                "popularity": data["popularity"],
                "genres": data["genres"],
            }
            cleanDict.append(clean)
        dfout = pd.DataFrame(cleanDict)
        return dfout
    else:
        logger = get_run_logger()
        logger.warning("Data not new, skipping fetching artist data...")


@task(name="Add Data to Tables")
def update_tables(newData, artistdf, playlistdf):
    # New data should be the boolean returned from check_data
    # Artist df is the df from the get artist data and playlistdf is the first df generated
    if newData:
        conn = psycopg.connect(conninfo=DB_CREDS)
        cursor = conn.cursor()

        artistExist = pd.read_sql("SELECT id FROM artists", engine)
        artistdfdup = artistdf
        artistdfdup = artistdfdup[~artistdfdup["id"].isin(artistExist["id"])]
        artistdfdup = artistdfdup.drop("genres", axis=1).drop_duplicates("id")
        artistdfdup.to_sql("artists", engine, if_exists="append", index=False)

        tracksExist = pd.read_sql("SELECT id FROM tracks", engine)
        tracksdf = playlistdf[
            ["id", "track_name","explicit", "primary_artist_id", "image", "duration_sec"]
        ]
        tracksdf = tracksdf[~tracksdf["id"].isin(tracksExist["id"])]
        tracksdf.to_sql("tracks", engine, if_exists="append", index=False)

        chartsdf = playlistdf[["date", "position", "id"]].rename(
            columns={"id": "track_id"}
        )
        chartsdf.to_sql(
            "chart_positions", engine, if_exists="append", index=False
        )

        # TODO handle artist genres
        genresdf = artistdf["genres"]
        genresdf = genresdf.rename("name")
        genresdf = genresdf[genresdf.astype(bool)]
        genresdf = genresdf.explode(ignore_index=True).drop_duplicates()
        genresExist = pd.read_sql("SELECT name FROM genres", engine)
        genresdf = genresdf[~genresdf.isin(genresExist["name"])]
        genresdf.to_sql("genres", engine, if_exists="append", index=False)

        artistGenresdf = (
            artistdf[["id", "genres"]]
            .explode("genres", ignore_index=True)
            .rename(columns={"id": "artist_id", "genres": "genre_name"})
        )
        artistGenresdf = artistGenresdf.dropna(how='any',axis=0)
        artistGenresdf.to_sql(
            "temporary_table", engine, if_exists="append", index=False
        )
        
        cursor.execute(
            "INSERT INTO artist_genres (artist_id, genre_name) SELECT artist_id, genre_name FROM temporary_table ON CONFLICT (artist_id, genre_name) DO NOTHING;"
        )
        cursor.execute("DROP TABLE IF EXISTS temporary_table")

        # TODO handle multiple artists not sure needed yet

        conn.commit()
        conn.close()
    else:
        logger = get_run_logger()
        logger.warning("Data not new, skipping table updates...")


@flow(log_prints=True)
def exec_flow():
    create_table()
    playdata = get_tracks()
    cont = check_data(playdata)
    save_hash(playdata)
    artdata = get_artist_data(cont, playdata)
    update_tables(cont, artdata, playdata)


if __name__ == "__main__":
    exec_flow.serve(cron="0 0 * * *")
