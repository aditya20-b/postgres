-- Test script for blockchain table access method

-- 1. Create a blockchain table
CREATE TABLE blockchain_test_table (id int, data text) USING blockchain;

-- Verify creation (implicitly, if no error)
-- Let's do a describe to see the structure and AM
\d blockchain_test_table

-- 2. Insert data into the blockchain table
INSERT INTO blockchain_test_table VALUES (1, 'First block');
INSERT INTO blockchain_test_table VALUES (2, 'Second block');
INSERT INTO blockchain_test_table VALUES (3, 'Third block');

-- 3. Query the blockchain table
SELECT * FROM blockchain_test_table ORDER BY id;

-- 4. Attempt to delete data from the blockchain table
-- This should fail
DELETE FROM blockchain_test_table WHERE id = 1;

-- 5. Attempt to update data in the blockchain table
-- This should fail
UPDATE blockchain_test_table SET data = 'Modified first block' WHERE id = 1;

-- Verify data is unchanged after failed attempts
SELECT * FROM blockchain_test_table ORDER BY id;

-- 6. Drop the blockchain table
DROP TABLE blockchain_test_table;

-- Verify drop (implicitly, if no error and table no longer exists)
\d blockchain_test_table
