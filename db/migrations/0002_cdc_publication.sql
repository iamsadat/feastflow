-- =========================================================

-- 0002_cdc_publication.sql: prepare the source for CDC (M1 step 5)

--

-- What this does:

--   1. REPLICA IDENTITY FULL on every business table, so UPDATE

--      and DELETE events carry the complete old row ("before").

--   2. A publication listing exactly the tables Debezium streams.

--

-- Rules:

--   * schema_migrations is NOT published: runner bookkeeping,

--     not business data.

--   * Tables are listed explicitly (not FOR ALL TABLES). A new

--     table is captured only when its migration runs

--     ALTER PUBLICATION feastflow_pub ADD TABLE ...

--

-- Needs wal_level=logical at runtime (set in docker-compose.yml);

-- this file itself runs at any wal_level.

-- =========================================================



ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE restaurants REPLICA IDENTITY FULL;
ALTER TABLE riders      REPLICA IDENTITY FULL;
ALTER TABLE menu_items  REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

CREATE PUBLICATION feastflow_pub FOR TABLE
    customers,
    restaurants,
    riders,
    menu_items,
    orders,
    order_items;