import re
import unicodedata

import numpy as np
import pandas as pd


# =========================================================
# 0. UTILIDADES (solo texto y reglas pequeñas)
# =========================================================

def normalizar_texto(valor):
    """Minúsculas, sin tildes y sin espacios repetidos. Sirve para comparar nombres."""
    if pd.isna(valor):
        return None
    texto = unicodedata.normalize("NFKD", str(valor)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", texto.lower()).strip() or None


def normalizar_titulo(valor):
    """Como normalizar_texto, pero quita paréntesis, 'feat.' y puntuación.
    Solo se usa para emparejar canciones y álbumes entre las dos fuentes."""
    texto = normalizar_texto(valor)
    if texto is None:
        return None
    texto = re.sub(r"\(.*?\)|\[.*?\]", " ", texto)
    texto = re.sub(r"\b(feat|ft|featuring)\b.*$", " ", texto)
    texto = re.sub(r"[^a-z0-9 ]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip() or None


def tipo_nominado(categoria):
    """Qué es el 'nominee' según la categoría: song, album, artist o unknown."""
    c = str(categoria).lower()
    if "new artist" in c:
        return "artist"
    if "album" in c:
        return "album"
    if any(k in c for k in ["song", "record of the year", "single", "performance",
                            "collaboration", "solo"]):
        return "song"
    if "recording" in c and not any(
        k in c for k in ["opera", "children", "comedy", "engineered", "package"]
    ):
        return "song"
    return "unknown"


# El orden es la prioridad: gana la primera coincidencia.
CAMPO_GENERAL = ["record of the year", "album of the year", "song of the year", "best new artist"]
GRUPOS_CATEGORIA = [
    (r"\br&b\b|rhythm", "R&B"),
    (r"\brap\b|hip hop", "Rap"),
    (r"\bcountry\b|bluegrass", "Country"),
    (r"\bjazz\b", "Jazz"),
    (r"\blatin\b|tropical|tejano|mexican", "Latin"),
    (r"(?<!non-)(?<!non )classical|opera|orchestra|chamber|choral", "Classical"),
    (r"gospel|christian", "Gospel"),
    (r"reggae", "Reggae"),
    (r"dance|electronic", "Dance/Electronic"),
    (r"\bfolk\b|americana|roots", "Folk/Roots"),
    (r"\bblues\b", "Blues"),
    (r"alternative", "Alternative"),
    (r"\brock\b|metal", "Rock"),
    (r"\bpop\b", "Pop"),
    (r"engineer|producer|package|notes|surround|remix|historical", "Production/Technical"),
    (r"children|comedy|spoken|musical show|musical album", "Children/Comedy/Theater"),
    (r"world music|new age|polka", "World/New Age"),
    (r"instrumental|arrangement|composition", "Instrumental/Composition"),
    (r"video", "Music Video"),
]


def grupo_categoria(categoria):
    c = str(categoria).lower()
    if any(k in c for k in CAMPO_GENERAL):
        return "General Field"
    for patron, grupo in GRUPOS_CATEGORIA:
        if re.search(patron, c):
            return grupo
    return "Other"


# =========================================================
# TRANSFORMACIÓN E INTEGRACIÓN
# =========================================================

def transform(df_spotify, df_grammys):
    """
    Entrada : Spotify (una fila por canción y género) y Grammy (una fila por nominado).
    Salida  : UN dataset integrado con las columnas del modelo dimensional.
              match_status indica de dónde viene cada fila:
                both          -> canción de Spotify con premio Grammy
                spotify_only  -> canción de Spotify sin premio
                grammy_only   -> ganador sin canción en Spotify
    """
    # Si una fuente llega vacía, es un problema de extracción: se detiene con un mensaje claro
    if df_spotify.empty:
        raise ValueError("Spotify llegó vacío a transform: revisar la extracción (ruta del CSV).")
    if df_grammys.empty:
        raise ValueError("Grammy llegó vacío a transform: revisar que source_db.grammys tenga filas.")

    df = df_spotify.copy()
    g = df_grammys.copy()

    # =========================================================
    # 1. SPOTIFY: dejar solo las columnas que usamos
    # =========================================================

    columnas_spotify = [
        "track_id", "track_name", "artists", "album_name", "track_genre",
        "popularity", "duration_ms", "explicit", "danceability", "energy",
    ]
    df = df[columnas_spotify]
    print("Spotify | filas de entrada:", len(df))

    # =========================================================
    # 2. SPOTIFY: limpieza (nulos, textos, tipos, duplicados)
    # =========================================================

    # Filas sin artista, álbum ni canción: no se pueden identificar -> se eliminan
    sin_identidad = df[["artists", "album_name", "track_name"]].isnull().all(axis=1)
    print("Spotify | filas sin artista, álbum ni canción (se eliminan):", int(sin_identidad.sum()))
    df = df[~sin_identidad]

    # Si queda algún nulo parcial, se conserva como "Unknown"
    for col in ["artists", "album_name", "track_name"]:
        df[col] = df[col].fillna("Unknown").astype(str).str.strip()

    df["track_id"] = df["track_id"].astype("string").str.strip()
    df["track_genre"] = df["track_genre"].astype("string").str.strip().str.lower()

    # Tipos de datos
    for col in ["popularity", "duration_ms"]:
        df[col] = pd.to_numeric(df[col], errors="raise")
    for col in ["danceability", "energy"]:
        df[col] = pd.to_numeric(df[col], errors="raise").astype(float)
    df["explicit"] = df["explicit"].astype(bool)

    # Un track_id aparece una vez por género. Solo se quita un duplicado exacto
    # (mismo track_id y mismo género), conservando el más popular.
    antes = len(df)
    df = (
        df.sort_values("popularity", ascending=False)
          .drop_duplicates(subset=["track_id", "track_genre"], keep="first")
    )
    print("Spotify | duplicados exactos eliminados:", antes - len(df))

    # =========================================================
    # 3. SPOTIFY: columnas derivadas
    # =========================================================

    # Artista principal: el primero de la lista "A;B;C"
    df["artists_original"] = df["artists"]   # solo se usa para emparejar con Grammy
    df["main_artists"] = (
        df["artists"].str.split(";").str[0].map(normalizar_texto).fillna("unknown")
    )
    df["artists"] = df["main_artists"]

    # Peso por género: una canción en 3 géneros pesa 1/3 en cada uno
    df["genre_weight"] = 1 / df.groupby("track_id")["track_genre"].transform("nunique")

    # Categoría de duración en terciles (Corta / Media / Larga).
    # Los cortes se calculan sobre canciones únicas para que una canción
    # con muchos géneros no pese más.
    canciones_unicas = df.drop_duplicates("track_id")
    corte_1, corte_2 = canciones_unicas["duration_ms"].quantile([1 / 3, 2 / 3])
    df["duration_category"] = pd.cut(
        df["duration_ms"],
        bins=[-np.inf, corte_1, corte_2, np.inf],
        labels=["Corta", "Media", "Larga"],
    ).astype(str)
    print(f"Spotify | cortes de duración: Corta <= {corte_1:.0f} ms < Media <= {corte_2:.0f} ms < Larga")

    # =========================================================
    # 4. GRAMMY: limpieza (textos, ganador, filas vacías)
    # =========================================================

    columnas_grammy = ["year", "category", "nominee", "artist", "winner"]
    if "id" in g.columns:                       # orden estable de la tabla fuente
        g = g.sort_values("id")
    g = g[columnas_grammy]
    print("Grammy | filas de entrada:", len(g))

    for col in ["category", "nominee", "artist"]:
        g[col] = (
            g[col].astype("string").str.strip()
            .str.replace(r"\s+", " ", regex=True).replace("", pd.NA)
        )
    g["category"] = g["category"].str.title()    # unifica "Non-classical" y "Non-Classical"
    g["year"] = pd.to_numeric(g["year"], errors="raise").astype("Int64")

    # La columna 'winner' es True en todas las filas, así que no sirve.
    # Regla: el ganador es la primera fila de cada (año, categoría).
    print("Grammy | valores distintos de 'winner':", g["winner"].nunique())
    g = g[~g.duplicated(subset=["year", "category"], keep="first")]
    print("Grammy | ganadores (una fila por año y categoría):", len(g))

    # Ganadores sin nominado ni artista: no hay información -> se eliminan.
    # (Se hace DESPUÉS de elegir al ganador, para no promover a otra fila.)
    sin_datos = g["nominee"].isna() & g["artist"].isna()
    print("Grammy | ganadores sin nominado ni artista (se eliminan):", int(sin_datos.sum()))
    g = g[~sin_datos]

    # =========================================================
    # 5. GRAMMY: columnas derivadas
    # =========================================================

    g["nominee_type"] = g["category"].map(tipo_nominado)
    g["category_group"] = g["category"].map(grupo_categoria)
    g["decade"] = (g["year"] // 10 * 10).astype("Int64")

    # Artista: si el nominado ES el artista (Best New Artist), se copia
    g["grammy_artists"] = g["artist"]
    es_artista = (g["nominee_type"] == "artist") & g["grammy_artists"].isna()
    g.loc[es_artista, "grammy_artists"] = g.loc[es_artista, "nominee"]

    # Artista faltante (p. ej. Song Of The Year): se toma de otra categoría del
    # mismo año con la misma canción (Record Of The Year sí trae al intérprete)
    g["titulo"] = g["nominee"].map(normalizar_titulo)
    referencia = (
        g.dropna(subset=["grammy_artists", "titulo"])
         .drop_duplicates(["year", "titulo"])[["year", "titulo", "grammy_artists"]]
         .rename(columns={"grammy_artists": "artista_ref"})
    )
    g = g.merge(referencia, on=["year", "titulo"], how="left")
    falta = g["grammy_artists"].isna() & g["artista_ref"].notna()
    g.loc[falta, "grammy_artists"] = g.loc[falta, "artista_ref"]

    g["grammy_artists"] = g["grammy_artists"].map(normalizar_texto).fillna("unknown")
    g["nominee"] = g["nominee"].fillna("Unknown")
    g["win_count"] = 1
    g["row_id"] = np.arange(len(g))
    print("Grammy | artista desconocido (%):", round(100 * (g["grammy_artists"] == "unknown").mean(), 1))

    # =========================================================
    # 6. INTEGRACIÓN: encontrar la canción y el álbum de cada ganador
    # =========================================================

    # Canciones únicas de Spotify. Se separan sus artistas para comparar con cualquiera.
    pistas = df.drop_duplicates("track_id")[
        ["track_id", "track_name", "artists_original", "main_artists", "album_name", "popularity"]
    ]
    llaves = pistas.assign(artista=pistas["artists_original"].str.split(";")).explode("artista")
    llaves["artista"] = llaves["artista"].map(normalizar_texto)
    llaves["titulo"] = llaves["track_name"].map(normalizar_titulo)

    # 6a. Ganadores tipo canción -> match por título + artista.
    # Si una canción tiene varios track_id (sencillo y álbum), se elige el más popular.
    canciones = g[(g["nominee_type"] == "song") & (g["grammy_artists"] != "unknown")]
    candidatos = canciones[["row_id", "titulo", "grammy_artists"]].merge(
        llaves, left_on=["titulo", "grammy_artists"], right_on=["titulo", "artista"]
    )
    mejor = (
        candidatos.sort_values("popularity", ascending=False)
                  .drop_duplicates("row_id")[["row_id", "track_id", "album_name", "main_artists"]]
                  .rename(columns={"album_name": "winner_album_name",
                                   "main_artists": "winner_album_artists"})
    )
    g = g.merge(mejor, on="row_id", how="left")

    # 6b. Ganadores tipo álbum -> match por nombre de álbum + artista
    tamano_album = pistas.groupby(["album_name", "main_artists"]).size().rename("n_pistas").reset_index()
    llaves_album = (
        llaves.drop_duplicates(["album_name", "main_artists", "artista"])
              [["album_name", "main_artists", "artista"]]
              .merge(tamano_album, on=["album_name", "main_artists"])
    )
    llaves_album = llaves_album[llaves_album["album_name"] != "Unknown"]
    llaves_album["titulo"] = llaves_album["album_name"].map(normalizar_titulo)

    albumes = g[(g["nominee_type"] == "album") & (g["grammy_artists"] != "unknown")]
    cand_album = albumes[["row_id", "titulo", "grammy_artists"]].merge(
        llaves_album, left_on=["titulo", "grammy_artists"], right_on=["titulo", "artista"]
    )
    mejor_album = (
        cand_album.sort_values("n_pistas", ascending=False)
                  .drop_duplicates("row_id")[["row_id", "album_name", "main_artists"]]
                  .rename(columns={"album_name": "album_2", "main_artists": "album_artists_2"})
    )
    g = g.merge(mejor_album, on="row_id", how="left")
    g["winner_album_name"] = g["winner_album_name"].fillna(g["album_2"])
    g["winner_album_artists"] = g["winner_album_artists"].fillna(g["album_artists_2"])

    for tipo, columna in [("song", "track_id"), ("album", "winner_album_name")]:
        filas = g[g["nominee_type"] == tipo]
        if len(filas):
            print(f"Integración | ganadores tipo {tipo} con match en Spotify (%):",
                  round(100 * filas[columna].notna().mean(), 1))

    # =========================================================
    # 7. UNIÓN: un solo dataset con las columnas del modelo
    # =========================================================

    columnas_spotify_final = [
        "track_id", "track_name", "explicit",                       # dim_track
        "artists", "album_name", "main_artists", "track_genre",     # dim_artists / dim_album / dim_genre
        "duration_category",                                        # dim_track
        "energy", "danceability", "popularity", "duration_ms", "genre_weight",   # fact_song
    ]
    columnas_grammy_final = [
        "year", "decade", "category", "category_group",             # dim_year / dim_category
        "nominee", "nominee_type", "grammy_artists", "win_count",   # dim_nominee / fact_winner
        "winner_album_name", "winner_album_artists",                # album al que se atribuye el premio
    ]

    spotify = df[columnas_spotify_final]
    ganadores = g[["track_id"] + columnas_grammy_final]

    con_match = ganadores[ganadores["track_id"].notna()]
    sin_match = ganadores[ganadores["track_id"].isna()].copy()

    # Merge por track_id: un ganador se repite una vez por cada género de su canción
    unido = spotify.merge(con_match, on="track_id", how="left")
    unido["match_status"] = np.where(unido["year"].notna(), "both", "spotify_only")

    # Los ganadores sin canción en Spotify se agregan aparte: no se pierden
    sin_match["match_status"] = "grammy_only"

    df_integrated = pd.concat([unido, sin_match], ignore_index=True)
    df_integrated = df_integrated[columnas_spotify_final + columnas_grammy_final + ["match_status"]]
    df_integrated["win_count"] = df_integrated["win_count"].fillna(0).astype(int)

    # Reconciliación: la unión no debe multiplicar ni perder filas de ninguna fuente
    filas_spotify = df_integrated[df_integrated["match_status"] != "grammy_only"]
    filas_grammy = df_integrated[df_integrated["match_status"] != "spotify_only"]
    spotify_ok = len(filas_spotify.drop_duplicates(["track_id", "track_genre"])) == len(df)
    grammy_ok = len(filas_grammy.drop_duplicates(["year", "category"])) == len(g)
    print("Unión | filas por estado:", df_integrated["match_status"].value_counts().to_dict())
    print("Unión | reconciliación Spotify:", spotify_ok, "| Grammy:", grammy_ok)
    if not (spotify_ok and grammy_ok):
        raise ValueError("La unión multiplicó o perdió filas: revisar el merge por track_id.")

    return df_integrated