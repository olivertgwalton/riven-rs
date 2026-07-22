-- Replace the string-prefix exclusion format (`!value`) with explicit
-- include/exclude collections in every saved filesystem library profile.
CREATE FUNCTION migrate_library_filter_selection(filter_values jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN jsonb_typeof(filter_values) = 'object' THEN filter_values
        WHEN jsonb_typeof(filter_values) = 'array' THEN jsonb_build_object(
            'include', COALESCE(
                jsonb_agg(entry) FILTER (WHERE entry NOT LIKE '!%'),
                '[]'::jsonb
            ),
            'exclude', COALESCE(
                jsonb_agg(substr(entry, 2)) FILTER (WHERE entry LIKE '!%'),
                '[]'::jsonb
            )
        )
        ELSE jsonb_build_object('include', '[]'::jsonb, 'exclude', '[]'::jsonb)
    END
    FROM jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(filter_values) = 'array' THEN filter_values
            ELSE '[]'::jsonb
        END
    ) AS elements(entry)
$$;

WITH migrated AS (
    SELECT jsonb_object_agg(
        profile_key,
        profile_value || jsonb_build_object(
            'filter_rules',
            COALESCE(profile_value -> 'filter_rules', '{}'::jsonb) || jsonb_build_object(
                'genres', migrate_library_filter_selection(profile_value #> '{filter_rules,genres}'),
                'networks', migrate_library_filter_selection(profile_value #> '{filter_rules,networks}'),
                'languages', migrate_library_filter_selection(profile_value #> '{filter_rules,languages}'),
                'countries', migrate_library_filter_selection(profile_value #> '{filter_rules,countries}'),
                'content_ratings', migrate_library_filter_selection(profile_value #> '{filter_rules,content_ratings}')
            )
        )
    ) AS profiles
    FROM settings
    CROSS JOIN LATERAL jsonb_each(
        CASE
            WHEN jsonb_typeof(value #> '{filesystem,library_profiles}') = 'object'
                THEN value #> '{filesystem,library_profiles}'
            ELSE '{}'::jsonb
        END
    ) AS profile(profile_key, profile_value)
    WHERE key = 'general'
)
UPDATE settings
SET value = jsonb_set(value, '{filesystem,library_profiles}', migrated.profiles)
FROM migrated
WHERE key = 'general'
  AND migrated.profiles IS NOT NULL;

DROP FUNCTION migrate_library_filter_selection(jsonb);
