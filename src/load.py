import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# 1. Configuración de rutas y variables de entorno (.env)
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUTA_CREATE_DW = PROJECT_ROOT / "sql" / "dw_schema.sql"

load_dotenv(PROJECT_ROOT / ".env")

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")


def crear_engine():
    """Crea el engine de SQLAlchemy para conectarse a PostgreSQL."""
    url = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(url)


def crear_esquema():
    """
    Ejecuta el archivo DDL (dw_schema_2.sql) para asegurar que
    el Data Warehouse y sus miembros Unknown (-1) existan.
    """
    ruta_sql = RUTA_CREATE_DW
    if not ruta_sql.exists():
        ruta_sql = PROJECT_ROOT / "sql" / "dw_schema.sql"

    if not ruta_sql.exists():
        raise FileNotFoundError(f"No se encontró el DDL SQL en: {RUTA_CREATE_DW}")

    with open(ruta_sql, "r", encoding="utf-8") as f:
        script_sql = f.read()

    engine = crear_engine()
    with engine.begin() as conexion:
        conexion.execute(text(script_sql))

    engine.dispose()
    print("✅ Esquema del Data Warehouse creado/verificado correctamente en PostgreSQL.")


def reset_schema():
    """Vacía los datos de las tablas conservando las llaves y la estructura DDL."""
    engine = crear_engine()
    tablas = [
        "fact_song", "fact_winner",
        "dim_track", "dim_artists", "dim_album", "dim_genre",
        "dim_year", "dim_category", "dim_nominee"
    ]
    with engine.begin() as conn:
        cmd = f"TRUNCATE TABLE {', '.join(tablas)} RESTART IDENTITY CASCADE;"
        try:
            conn.execute(text(cmd))
            print("🧹 Tablas del Data Warehouse vaciadas con éxito.")
        except Exception as e:
            print(f"Nota en reset (no crítico): {e}")
    engine.dispose()


# ---------------------------------------------------------
# 2. Función Principal de Carga (load_dw)
# ---------------------------------------------------------

def load_dw(df_integrated: pd.DataFrame):
    """
    Recibe el DataFrame integrado de la fase de transform, poblando las 
    7 dimensiones y calculando las llaves sustitutas (_key) para los hechos.
    """
    reset_schema()
    
    crear_esquema()
    engine = crear_engine()

    print("\n🚀 Iniciando proceso de carga al Data Warehouse...")

    # =========================================================
    # PASO A: POBLAR DIMENSIONES Y CONSTRUIR DICCIONARIOS DE MAPPING
    # =========================================================

    # 1. dim_artists
    todos_artistas = pd.concat([
        df_integrated["artists"],
        df_integrated["main_artists"],
        df_integrated["grammy_artists"],
        df_integrated["winner_album_artists"]
    ]).dropna().unique()
    artistas_df = pd.DataFrame({"artists": [a for a in todos_artistas if a not in ["unknown", "Unknown"]]}).drop_duplicates()
    
    artistas_df.to_sql("dim_artists", engine, if_exists="append", index=False, method="multi")
    artistas_db = pd.read_sql("SELECT artists_key, artists FROM dim_artists;", engine)
    map_artists = dict(zip(artistas_db["artists"], artistas_db["artists_key"]))

    # 2. dim_track
    spot_tracks = df_integrated[df_integrated["match_status"] != "grammy_only"].drop_duplicates("track_id")
    track_df = spot_tracks[["track_name", "track_id", "explicit", "duration_category"]]
    track_df.to_sql("dim_track", engine, if_exists="append", index=False, method="multi")
    track_db = pd.read_sql("SELECT track_key, track_id FROM dim_track;", engine)
    map_track = dict(zip(track_db["track_id"], track_db["track_key"]))

    # 3. dim_album
    album_df = df_integrated[["album_name", "main_artists"]].dropna().drop_duplicates()
    album_df = album_df[album_df["album_name"] != "Unknown"]
    album_df.to_sql("dim_album", engine, if_exists="append", index=False, method="multi")
    album_db = pd.read_sql("SELECT album_key, album_name, main_artists FROM dim_album;", engine)
    map_album = dict(zip(zip(album_db["album_name"], album_db["main_artists"]), album_db["album_key"]))

    # 4. dim_genre
    genres = df_integrated["track_genre"].dropna().unique()
    genre_df = pd.DataFrame({"track_genre": [g for g in genres if g != "unknown"]}).drop_duplicates()
    genre_df.to_sql("dim_genre", engine, if_exists="append", index=False, method="multi")
    genre_db = pd.read_sql("SELECT genre_key, track_genre FROM dim_genre;", engine)
    map_genre = dict(zip(genre_db["track_genre"], genre_db["genre_key"]))

    # 5. dim_year
    years_df = df_integrated[["year", "decade"]].dropna().drop_duplicates()
    years_df["year"] = years_df["year"].astype(int)
    years_df["decade"] = years_df["decade"].astype(int)
    years_df.to_sql("dim_year", engine, if_exists="append", index=False, method="multi")
    year_db = pd.read_sql("SELECT year_key, year FROM dim_year;", engine)
    map_year = dict(zip(year_db["year"], year_db["year_key"]))

    # 6. dim_category
    cat_df = df_integrated[["category", "category_group"]].dropna().drop_duplicates()
    cat_df.to_sql("dim_category", engine, if_exists="append", index=False, method="multi")
    cat_db = pd.read_sql("SELECT category_key, category FROM dim_category;", engine)
    map_category = dict(zip(cat_db["category"], cat_db["category_key"]))

    # 7. dim_nominee
    nom_df = df_integrated[["nominee", "nominee_type"]].dropna().drop_duplicates()
    nom_df.to_sql("dim_nominee", engine, if_exists="append", index=False, method="multi")
    nom_db = pd.read_sql("SELECT nominee_key, nominee, nominee_type FROM dim_nominee;", engine)
    map_nominee = dict(zip(zip(nom_db["nominee"], nom_db["nominee_type"]), nom_db["nominee_key"]))

    print("✅ Dimensiones cargadas y mapeadas exitosamente.")

    # =========================================================
    # PASO B: POBLAR TABLA DE HECHOS 1 (fact_song)
    # =========================================================

    songs_data = df_integrated[df_integrated["match_status"] != "grammy_only"].copy()
    
    fact_song = pd.DataFrame({
        "track_key": songs_data["track_id"].map(map_track).fillna(-1).astype(int),
        "artists_key": songs_data["artists"].map(map_artists).fillna(-1).astype(int),
        "album_key": songs_data.apply(lambda r: map_album.get((r["album_name"], r["main_artists"]), -1), axis=1),
        "genre_key": songs_data["track_genre"].map(map_genre).fillna(-1).astype(int),
        "energy": songs_data["energy"],
        "danceability": songs_data["danceability"],
        "popularity": songs_data["popularity"],
        "duration_ms": songs_data["duration_ms"],
        "genre_weight": songs_data["genre_weight"]
    }).drop_duplicates(subset=["track_key", "genre_key", "artists_key"])

    fact_song.to_sql("fact_song", engine, if_exists="append", index=False, method="multi")
    print(f"✅ Fact Table 'fact_song' cargada con {len(fact_song):,} registros.")

    # =========================================================
    # PASO C: POBLAR TABLA DE HECHOS 2 (fact_winner)
    # =========================================================

    winners_data = df_integrated[df_integrated["match_status"] != "spotify_only"].copy()

    fact_winner = pd.DataFrame({
        "year_key": winners_data["year"].map(map_year).fillna(-1).astype(int),
        "category_key": winners_data["category"].map(map_category).fillna(-1).astype(int),
        "nominee_key": winners_data.apply(lambda r: map_nominee.get((r["nominee"], r["nominee_type"]), -1), axis=1),
        "artists_key": winners_data["grammy_artists"].map(map_artists).fillna(-1).astype(int),
        "track_key": winners_data["track_id"].map(map_track).fillna(-1).astype(int),
        "album_key": winners_data.apply(lambda r: map_album.get((r["winner_album_name"], r["winner_album_artists"]), -1), axis=1),
        "win_count": winners_data["win_count"].fillna(1).astype(int)
    }).drop_duplicates(subset=["year_key", "category_key", "nominee_key", "artists_key"])

    fact_winner.to_sql("fact_winner", engine, if_exists="append", index=False, method="multi")
    print(f"✅ Fact Table 'fact_winner' cargada con {len(fact_winner):,} registros.")

    engine.dispose()
    print("🎉 ¡Carga completa del Data Warehouse finalizada con éxito!")