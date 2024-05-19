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