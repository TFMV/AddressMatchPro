CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    street VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20)
);

CREATE TABLE reference_entities (
    id SERIAL PRIMARY KEY,
    entity_value TEXT
);

CREATE TABLE customer_keys (
    customer_id INT,
    binary_key CHAR(20),
    FOREIGN KEY (customer_id) REFERENCES customers (id)
);

CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    hash VARCHAR(255),
    number VARCHAR(50),
    street VARCHAR(255),
    unit VARCHAR(50),
    city VARCHAR(255),
    district VARCHAR(255),
    region VARCHAR(50),
    postcode VARCHAR(20),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

INSERT INTO public.reference_entities (ID, entity_value) VALUES
(1, '742 evergreen ter'),
(2, '1234 elm st apt 2b'),
(3, '500 w madison st ste 1500'),
(4, '2020 maple ave unit 12'),
(5, '99 oak ridge dr'),
(6, '1600 pennsylvania ave nw'),
(7, '350 fifth ave fl 22'),
(8, '47-20 bell blvd'),
(9, '2000 richmond hwy'),
(10, 'po box 9876'),
(11, '100 main st'),
(12, '456 broadway ave'),
(13, '789 market st'),
(14, '321 park blvd'),
(15, '654 oak st'),
(16, '987 maple rd'),
(17, '135 elm dr'),
(18, '246 pine ln'),
(19, '357 cedar ct'),
(20, '468 birch pl');


SELECT 
    SUBSTRING(binary_key FROM 1 FOR 1) AS position_1,
    SUBSTRING(binary_key FROM 2 FOR 1) AS position_2,
    SUBSTRING(binary_key FROM 3 FOR 1) AS position_3,
    SUBSTRING(binary_key FROM 4 FOR 1) AS position_4,
    SUBSTRING(binary_key FROM 5 FOR 1) AS position_5,
    SUBSTRING(binary_key FROM 6 FOR 1) AS position_6,
    SUBSTRING(binary_key FROM 7 FOR 1) AS position_7,
    SUBSTRING(binary_key FROM 8 FOR 1) AS position_8,
    SUBSTRING(binary_key FROM 9 FOR 1) AS position_9,
    SUBSTRING(binary_key FROM 10 FOR 1) AS position_10,
    SUBSTRING(binary_key FROM 11 FOR 1) AS position_11,
    SUBSTRING(binary_key FROM 12 FOR 1) AS position_12,
    SUBSTRING(binary_key FROM 13 FOR 1) AS position_13,
    SUBSTRING(binary_key FROM 14 FOR 1) AS position_14,
    SUBSTRING(binary_key FROM 15 FOR 1) AS position_15,
    SUBSTRING(binary_key FROM 16 FOR 1) AS position_16,
    SUBSTRING(binary_key FROM 17 FOR 1) AS position_17,
    SUBSTRING(binary_key FROM 18 FOR 1) AS position_18,
    SUBSTRING(binary_key FROM 19 FOR 1) AS position_19,
    SUBSTRING(binary_key FROM 20 FOR 1) AS position_20,
    COUNT(*) AS count
FROM customer_keys
GROUP BY 
    position_1, position_2, position_3, position_4, position_5, 
    position_6, position_7, position_8, position_9, position_10,
    position_11, position_12, position_13, position_14, position_15,
    position_16, position_17, position_18, position_19, position_20
ORDER BY count DESC;
