CREATE INDEX ix_aggregates_security_authorizations
ON aggregates
USING GIN ((content -> 'authorizations') jsonb_path_ops)
WHERE key = 'security';
