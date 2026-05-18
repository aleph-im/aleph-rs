-- Drop the old arrow-operator index
DROP INDEX IF EXISTS ix_messages_content_tags_gin;

-- Recreate with subscript syntax to match SQLAlchemy-generated queries
CREATE INDEX IF NOT EXISTS ix_messages_content_tags_gin
ON messages USING GIN ((content['content']['tags']))
WHERE type = 'POST';
