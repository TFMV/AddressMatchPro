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
    binary_key CHAR(10),
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
(10, 'po box 9876');