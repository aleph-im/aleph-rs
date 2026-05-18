CREATE INDEX IF NOT EXISTS ix_posts_owner_type_channel
ON posts (owner, type, channel)
WHERE amends IS NULL;
