CREATE INDEX ix_messages_posts_type_tags
ON messages((content->>'type'),(content->'content'->>'tags')) WHERE type = 'POST';
