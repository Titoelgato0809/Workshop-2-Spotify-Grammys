-- 1. Crear esquema si no existe
CREATE SCHEMA IF NOT EXISTS source_db;

-- 2. Eliminar tabla previa para limpiar la caché del sistema
DROP TABLE IF EXISTS source_db.grammys;

-- 3. Crear la tabla con las 10 columnas exactas y sus tipos de datos
CREATE TABLE source_db.grammys (
    year INT,
    title VARCHAR(500),
    published_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    category VARCHAR(500),
    nominee VARCHAR(500),
    artist VARCHAR(500),
    workers TEXT,
    img TEXT,
    winner BOOLEAN
);

-- Carga automática desde el archivo montado en la ruta interna del contenedor
-- NOTA: Se usan las 10 columnas y la ruta absoluta '/data/...'
COPY source_db.grammys(year, title, published_at, updated_at, category, nominee, artist, workers, img, winner)
FROM '/data/the_grammy_awards.csv'
DELIMITER ','
CSV HEADER;