ALTER TABLE account_costs ADD COLUMN cost_credit DECIMAL NOT NULL DEFAULT 0;

INSERT INTO error_codes(code, description) VALUES
    (6, 'Insufficient credit')
ON CONFLICT (code) DO NOTHING;
