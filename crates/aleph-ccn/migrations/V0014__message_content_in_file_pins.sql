-- We now store a file + file pin object for the content of each non-inline message.

-- The existing unique constraint on item_hash will fail because of some non-inline STORE messages.
ALTER TABLE file_pins DROP CONSTRAINT file_pins_item_hash_key;
ALTER TABLE file_pins ADD CONSTRAINT file_pins_item_hash_type_key UNIQUE (item_hash, type);

INSERT INTO files(hash, size, type)
    SELECT messages.item_hash, messages.size, 'file'
    FROM messages WHERE item_type != 'inline';

INSERT INTO file_pins(file_hash, created, type, tx_hash, owner, item_hash, ref)
SELECT  messages.item_hash,
        to_timestamp((messages.content ->> 'time')::float),
        'content',
        null,
        messages.sender,
        messages.item_hash,
        null
FROM messages
WHERE item_type != 'inline';
