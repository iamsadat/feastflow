ALTER TABLE customers DROP CONSTRAINT customers_city_check;
ALTER TABLE customers ADD CONSTRAINT customers_city_check CHECK (city IN ('Hyderabad', 'Bengaluru', 'Chennai'));