-- Denormalize message and post tags into TEXT[] columns
-- Promotes the tag list out of JSONB into native TEXT[] columns on
-- messages and posts so that filtering uses a single GIN index that
-- covers every message type.

-- Step 1: schema additions
ALTER TABLE messages ADD COLUMN IF NOT EXISTS tags TEXT[];
ALTER TABLE posts ADD COLUMN IF NOT EXISTS tags TEXT[];

-- Step 2: backfill messages.tags from legacy JSONB locations
UPDATE messages
SET tags = CASE
    WHEN type IN ('POST', 'AGGREGATE')
        THEN ARRAY(SELECT jsonb_array_elements_text(content->'content'->'tags'))
    WHEN type = 'STORE'
        THEN ARRAY(SELECT jsonb_array_elements_text(content->'tags'))
    WHEN type IN ('INSTANCE', 'PROGRAM')
        THEN ARRAY(SELECT jsonb_array_elements_text(content->'metadata'->'tags'))
END
WHERE tags IS NULL
  AND (
    (type IN ('POST', 'AGGREGATE')
        AND jsonb_typeof(content->'content'->'tags') = 'array'
        AND content->'content'->'tags' <> '[]'::jsonb)
    OR (type = 'STORE'
        AND jsonb_typeof(content->'tags') = 'array'
        AND content->'tags' <> '[]'::jsonb)
    OR (type IN ('INSTANCE', 'PROGRAM')
        AND jsonb_typeof(content->'metadata'->'tags') = 'array'
        AND content->'metadata'->'tags' <> '[]'::jsonb)
  );

-- Backfill posts.tags
UPDATE posts
SET tags = ARRAY(SELECT jsonb_array_elements_text(content->'tags'))
WHERE tags IS NULL
  AND jsonb_typeof(content->'tags') = 'array'
  AND content->'tags' <> '[]'::jsonb;

-- Step 3: build GIN indexes, drop the obsolete one
CREATE INDEX IF NOT EXISTS ix_messages_tags_gin
    ON messages USING GIN (tags) WHERE tags IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_posts_tags_gin
    ON posts USING GIN (tags) WHERE tags IS NOT NULL;
DROP INDEX IF EXISTS ix_messages_content_tags_gin;
