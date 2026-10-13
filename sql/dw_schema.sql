-- =====================================================================
-- Data Warehouse: constelación de hechos (Spotify + Grammy)
-- Motor: PostgreSQL
-- Dimensiones conformadas: dim_artists, dim_track
-- Nombres alineados con el diagrama final
-- =====================================================================

-- ---------------------------------------------------------------------
-- DIMENSIONES
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_artists (
    artists_key  SERIAL PRIMARY KEY,
    artists      TEXT NOT NULL UNIQUE          -- normalizado en la transformación
);

CREATE TABLE IF NOT EXISTS dim_track (
    track_key   SERIAL PRIMARY KEY,
    track_name  TEXT,
    track_id    VARCHAR(30) NOT NULL UNIQUE,   -- clave de negocio (Spotify)
    explicit    BOOLEAN,
    duration_category  VARCHAR(10)                -- Corta | Media | Larga (terciles)
);

CREATE TABLE IF NOT EXISTS dim_album (
    album_key     SERIAL PRIMARY KEY,
    album_name    TEXT NOT NULL,
    main_artists  TEXT NOT NULL,
    CONSTRAINT uq_dim_album UNIQUE (album_name, main_artists)
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key    SERIAL PRIMARY KEY,
    track_genre  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_key  SERIAL PRIMARY KEY,
    year      INT NOT NULL UNIQUE,
    decade    INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key    SERIAL PRIMARY KEY,
    category        TEXT NOT NULL UNIQUE,
    category_group  TEXT
);

CREATE TABLE IF NOT EXISTS dim_nominee (
    nominee_key   SERIAL PRIMARY KEY,
    nominee       TEXT NOT NULL,
    nominee_type  VARCHAR(10) NOT NULL
        CHECK (nominee_type IN ('song', 'album', 'artist', 'unknown')),
    CONSTRAINT uq_dim_nominee UNIQUE (nominee, nominee_type)
);

-- ---------------------------------------------------------------------
-- Miembros "Unknown" (key = -1) para nulos y casos sin match
-- ---------------------------------------------------------------------

INSERT INTO dim_artists  (artists_key, artists)                       VALUES (-1, 'Unknown')                  ON CONFLICT DO NOTHING;
INSERT INTO dim_track    (track_key, track_name, track_id, explicit)  VALUES (-1, 'Unknown', 'UNKNOWN', NULL) ON CONFLICT DO NOTHING;
INSERT INTO dim_album    (album_key, album_name, main_artists)        VALUES (-1, 'Unknown', 'Unknown')       ON CONFLICT DO NOTHING;
INSERT INTO dim_genre    (genre_key, track_genre)                     VALUES (-1, 'Unknown')                  ON CONFLICT DO NOTHING;
INSERT INTO dim_category (category_key, category, category_group)     VALUES (-1, 'Unknown', 'Unknown')       ON CONFLICT DO NOTHING;
INSERT INTO dim_nominee  (nominee_key, nominee, nominee_type)         VALUES (-1, 'Unknown', 'unknown')       ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------
-- HECHO 1: Spotify
-- Grano: una fila por canción por género (por artista principal)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_song (
    song_id       BIGSERIAL PRIMARY KEY,
    track_key     INT NOT NULL REFERENCES dim_track(track_key),
    artists_key   INT NOT NULL REFERENCES dim_artists(artists_key),
    album_key     INT NOT NULL REFERENCES dim_album(album_key),
    genre_key     INT NOT NULL REFERENCES dim_genre(genre_key),
    energy        NUMERIC(4,3),
    danceability  NUMERIC(4,3),
    popularity    SMALLINT,
    duration_ms   INT,
    genre_weight  NUMERIC(5,4),                -- 1 / nº de géneros de la canción
    CONSTRAINT uq_fact_song UNIQUE (track_key, genre_key, artists_key)
);

-- ---------------------------------------------------------------------
-- HECHO 2: Grammy
-- Grano: una fila por ganador, categoría y año
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_winner (
    winner_id     BIGSERIAL PRIMARY KEY,
    year_key      INT NOT NULL REFERENCES dim_year(year_key),
    category_key  INT NOT NULL REFERENCES dim_category(category_key),
    nominee_key   INT NOT NULL REFERENCES dim_nominee(nominee_key),
    artists_key   INT NOT NULL REFERENCES dim_artists(artists_key),   -- -1 si no se puede derivar
    track_key     INT NOT NULL REFERENCES dim_track(track_key),       -- -1 si no es canción o no hay match
    album_key     INT NOT NULL REFERENCES dim_album(album_key),       -- álbum al que se atribuye el premio; -1 si no hay match
    win_count     SMALLINT NOT NULL DEFAULT 1,
    CONSTRAINT uq_fact_winner UNIQUE (year_key, category_key, nominee_key, artists_key)
);

-- ---------------------------------------------------------------------
-- Índices sobre FK
-- ---------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS ix_fact_song_track    ON fact_song (track_key);
CREATE INDEX IF NOT EXISTS ix_fact_song_genre    ON fact_song (genre_key);
CREATE INDEX IF NOT EXISTS ix_fact_song_artists  ON fact_song (artists_key);
CREATE INDEX IF NOT EXISTS ix_fact_song_album    ON fact_song (album_key);
CREATE INDEX IF NOT EXISTS ix_fact_winner_track   ON fact_winner (track_key);
CREATE INDEX IF NOT EXISTS ix_fact_winner_artists ON fact_winner (artists_key);
CREATE INDEX IF NOT EXISTS ix_fact_winner_year    ON fact_winner (year_key);
CREATE INDEX IF NOT EXISTS ix_fact_winner_cat     ON fact_winner (category_key);
CREATE INDEX IF NOT EXISTS ix_fact_winner_album   ON fact_winner (album_key);