ALTER TABLE media_items
ADD COLUMN IF NOT EXISTS is_anime BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE media_items
SET is_anime = TRUE
WHERE (
        EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(COALESCE(genres, '[]'::jsonb)) AS genre(value)
            WHERE lower(genre.value) = 'anime'
        )
    )
   OR (
        EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(COALESCE(genres, '[]'::jsonb)) AS genre(value)
            WHERE lower(genre.value) = 'animation'
        )
        AND lower(COALESCE(language, '')) NOT IN ('', 'en', 'eng')
   );
