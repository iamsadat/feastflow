-- SCD2 drill (M3 step 6): scripted changes whose history we can predict exactly.
-- Each statement is its own transaction (autocommit), so each gets its own LSN.

-- Customer 12 moves: address and zip are tracked, so this opens a new version.
UPDATE customers SET address = '12 Lake View Road', zip = '500081', updated_at = now()
WHERE customer_id = 12;

-- Customer 12 changes phone: not tracked, so the current version is updated in place.
UPDATE customers SET phone = '9000000999', updated_at = now()
WHERE customer_id = 12;

-- Restaurant 5 is re-rated twice: rating is tracked, so two new versions.
-- The two commits can fall in the same millisecond; the LSN in sequence_by breaks the tie.
UPDATE restaurants SET rating = 4.1, updated_at = now() WHERE restaurant_id = 5;
UPDATE restaurants SET rating = 4.4, updated_at = now() WHERE restaurant_id = 5;

-- Restaurant 5 is renamed: name is not tracked, so it changes the current version in place.
UPDATE restaurants SET name = name || ' Express', updated_at = now() WHERE restaurant_id = 5;
