CREATE TABLE error_codes (
    code INTEGER NOT NULL,
    description VARCHAR NOT NULL,
    PRIMARY KEY (code)
);

ALTER TABLE rejected_messages DROP COLUMN reason;
ALTER TABLE rejected_messages ADD COLUMN error_code INTEGER NOT NULL;
ALTER TABLE rejected_messages ADD COLUMN details JSONB;

INSERT INTO error_codes(code, description) VALUES
    (-1, 'Internal error'),
    (0, 'Invalid message format'),
    (1, 'Invalid signature'),
    (2, 'Permission denied'),
    (3, 'Message content unavailable'),
    (4, 'File unavailable'),
    (100, 'Amend post ref field is empty'),
    (101, 'Cannot find original ref of amend post'),
    (102, 'Amend post cannot amend another amend'),
    (500, 'No FORGET target specified'),
    (501, 'FORGET target not found'),
    (502, 'Cannot forget a FORGET message');

CREATE VIEW rejected_messages_view AS
SELECT rejected_messages.*, description FROM rejected_messages
LEFT OUTER JOIN error_codes
ON rejected_messages.error_code = error_codes.code;
