-- Schema checks for 0001_init.sql. Run against a fresh database
-- where only 0001 has been applied. Each block should behave as
-- its comment says.

\echo '== positive: one valid row in every table =='
INSERT INTO customers (name, phone, address, city, zip)
VALUES ('Asha Rao', '+919800000001', 'Road 12, Banjara Hills', 'Hyderabad', '500034')
RETURNING customer_id;

INSERT INTO restaurants (name, address, phone, city, opens_at, closes_at)
VALUES ('Paradise Biryani', 'MG Road, Secunderabad', '+914000000001', 'Hyderabad', '11:00', '23:30')
RETURNING restaurant_id, rating;

INSERT INTO riders (name, phone, address, city)
VALUES ('Ravi Kumar', '+919800000002', 'Ameerpet', 'Hyderabad')
RETURNING rider_id, status;

INSERT INTO menu_items (restaurant_id, name, price_paise)
VALUES (1, 'Chicken Biryani', 34900)
RETURNING item_id;

INSERT INTO orders (customer_id, restaurant_id, total_paise)
VALUES (1, 1, 34900)
RETURNING order_id, status;

INSERT INTO order_items (order_id, item_id, quantity, unit_price_paise)
VALUES (1, 1, 1, 34900)
RETURNING order_item_id;

\echo '== negative: bad status (expect orders_status_check) =='
INSERT INTO orders (customer_id, restaurant_id, total_paise, status)
VALUES (1, 1, 34900, 'LOST');

\echo '== negative: unknown menu item on a real order (expect order_items_item_id_fkey) =='
INSERT INTO order_items (order_id, item_id, quantity, unit_price_paise)
VALUES (1, 999, 1, 34900);

\echo '== negative: deleting a sold menu item (expect RESTRICT on order_items_item_id_fkey) =='
DELETE FROM menu_items WHERE item_id = 1;

\echo '== cascade: purging the order removes its line =='
BEGIN;
DELETE FROM orders WHERE order_id = 1;
SELECT count(*) AS lines_left FROM order_items WHERE order_id = 1;
ROLLBACK;

\echo '== trigger: updated_at moves on UPDATE (separate transaction) =='
SELECT pg_sleep(1);
UPDATE restaurants SET name = 'Paradise Biryani (Secunderabad)'
WHERE restaurant_id = 1
RETURNING created_at, updated_at, updated_at > created_at AS trigger_fired;
