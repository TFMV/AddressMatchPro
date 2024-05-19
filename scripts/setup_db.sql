CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone_number VARCHAR(20),
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