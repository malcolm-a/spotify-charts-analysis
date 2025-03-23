-- CREATE SCHEMA benchmark;

DROP TABLE IF EXISTS benchmark.artist_song;
DROP TABLE IF EXISTS benchmark.song;
DROP TABLE IF EXISTS benchmark.artist;

CREATE TABLE benchmark.artist (
    spotify_id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE benchmark.song (
    song_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    artist_ids JSONB  -- store artist ids as a JSONB array
);

CREATE TABLE benchmark.artist_song (
    artist_id TEXT REFERENCES benchmark.artist(spotify_id),
    song_id TEXT REFERENCES benchmark.song(song_id),
    PRIMARY KEY (artist_id, song_id)
);


-- INSERT REDUCED DATASET
INSERT INTO benchmark.artist (spotify_id, name)
SELECT spotify_id, name
FROM public.artist
WHERE UPPER(name) IN (
    'DRAKE', 'PARTYNEXTDOOR', 'RIHANNA', 'KENDRICK LAMAR', 
    'PLAYBOI CARTI', 'SZA', 'TYLER, THE CREATOR', 
    'KALI UCHIS', 'FRANK OCEAN'
);

INSERT INTO benchmark.song (song_id, name)
SELECT DISTINCT s.song_id, s.name
FROM public.song s
JOIN public.artist_song ars ON s.song_id = ars.song_id
JOIN public.artist a ON ars.artist_id = a.spotify_id
WHERE UPPER(a.name) IN (
    'DRAKE', 'PARTYNEXTDOOR', 'RIHANNA', 'KENDRICK LAMAR', 
    'PLAYBOI CARTI', 'SZA', 'TYLER, THE CREATOR', 
    'KALI UCHIS', 'FRANK OCEAN'
);

INSERT INTO benchmark.artist_song (artist_id, song_id)
SELECT ars.artist_id, ars.song_id
FROM public.artist_song ars
JOIN benchmark.artist a ON ars.artist_id = a.spotify_id
JOIN benchmark.song s ON ars.song_id = s.song_id;


-- FILL JSONB ARTIST_IDS COLUMN
UPDATE benchmark.song s
SET artist_ids = jsonb_build_object(
    'artists', (
        SELECT jsonb_agg(jsonb_build_object('id', a.spotify_id, 'name', a.name))
        FROM benchmark.artist_song ars
        JOIN benchmark.artist a ON ars.artist_id = a.spotify_id
        WHERE ars.song_id = s.song_id
    )
);

-- INDEXES

    -- RELATIONAL
CREATE INDEX idx_artist_song_song_id ON benchmark.artist_song(song_id);
CREATE INDEX idx_artist_song_artist_id ON benchmark.artist_song(artist_id);
CREATE INDEX idx_artist_name ON benchmark.artist(LOWER(name));

    -- JSONB
CREATE INDEX idx_song_artist_ids ON benchmark.song USING GIN (artist_ids);


-- BENCHMARK

	-- RELATIONAL
EXPLAIN ANALYZE
SELECT 
    a1.name AS artist1_name,
    a2.name AS artist2_name,
    COUNT(*) AS collaboration_count
FROM benchmark.artist_song ars1
JOIN benchmark.artist_song ars2 ON ars1.song_id = ars2.song_id
JOIN benchmark.artist a1 ON ars1.artist_id = a1.spotify_id
JOIN benchmark.artist a2 ON ars2.artist_id = a2.spotify_id
WHERE a1.spotify_id < a2.spotify_id  
GROUP BY a1.name, a2.name
ORDER BY collaboration_count DESC;

	-- JSONB
EXPLAIN ANALYZE
WITH collaborations AS (
    SELECT
        artist1->>'id' AS artist1_id,
        artist1->>'name' AS artist1_name,
        artist2->>'id' AS artist2_id,
        artist2->>'name' AS artist2_name,
        COUNT(*) AS collaboration_count
    FROM benchmark.song,
    LATERAL jsonb_array_elements(artist_ids->'artists') AS artist1,
    LATERAL jsonb_array_elements(artist_ids->'artists') AS artist2
    WHERE artist1->>'id' < artist2->>'id'  -- Ensures consistent ordering (A B but not B A)
    GROUP BY artist1->>'id', artist1->>'name', artist2->>'id', artist2->>'name'
)
SELECT
    artist1_name,
    artist2_name,
    collaboration_count
FROM collaborations
ORDER BY collaboration_count DESC;