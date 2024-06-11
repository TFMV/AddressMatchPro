-- --------------------------------------------------------------------------------
-- Author: Thomas F McGeehan V
--
-- This file is part of a software project developed by Thomas F McGeehan V.
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.
--
-- For more information about the MIT License, please visit:
-- https://opensource.org/licenses/MIT
--
-- Acknowledgment appreciated but not required.
-- --------------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS vector;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS customer_vector_embedding_default;
DROP TABLE IF EXISTS customer_vector_embedding_run_0;
DROP TABLE IF EXISTS customer_vector_embedding;
DROP TABLE IF EXISTS tokens_idf_default;
DROP TABLE IF EXISTS tokens_idf_run_0;
DROP TABLE IF EXISTS tokens_idf;
DROP TABLE IF EXISTS customer_tokens_default;
DROP TABLE IF EXISTS customer_tokens_run_0;
DROP TABLE IF EXISTS customer_tokens;
DROP TABLE IF EXISTS customer_keys_default;
DROP TABLE IF EXISTS customer_keys_run_0;
DROP TABLE IF EXISTS customer_keys;
DROP TABLE IF EXISTS customer_matching;
DROP TABLE IF EXISTS reference_entities;

-- Create tables
CREATE TABLE reference_entities (
    ID SERIAL PRIMARY KEY,
    entity_value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_matching (
    customer_id SERIAL,
    first_name TEXT,
    last_name TEXT,
    phone_number TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    run_id INT,
    PRIMARY KEY (customer_id, run_id)
);

CREATE TABLE IF NOT EXISTS batch_match (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    phone_number TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT
);

CREATE TABLE IF NOT EXISTS customer_keys (
    customer_id INT,
    binary_key TEXT,
    run_id INT NOT NULL
) PARTITION BY LIST (run_id);

CREATE TABLE IF NOT EXISTS customer_keys_run_0 PARTITION OF customer_keys FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS customer_keys_default PARTITION OF customer_keys DEFAULT;

CREATE TABLE IF NOT EXISTS customer_tokens (
    customer_id INT,
    entity_type_id INT,
    ngram_token TEXT,
    ngram_tfidf FLOAT8,
    run_id INT NOT NULL
) PARTITION BY LIST (run_id);

CREATE TABLE IF NOT EXISTS customer_tokens_run_0 PARTITION OF customer_tokens FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS customer_tokens_default PARTITION OF customer_tokens DEFAULT;

CREATE TABLE IF NOT EXISTS tokens_idf (
    entity_type_id INT,
    ngram_token TEXT,
    ngram_idf FLOAT8,
    run_id INT NOT NULL
) PARTITION BY LIST (run_id);

CREATE TABLE IF NOT EXISTS tokens_idf_run_0 PARTITION OF tokens_idf FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS tokens_idf_default PARTITION OF tokens_idf DEFAULT;

CREATE TABLE IF NOT EXISTS customer_vector_embedding (
    customer_id INT,
    vector_embedding VECTOR(300),
    run_id INT NOT NULL
) PARTITION BY LIST (run_id);

CREATE TABLE IF NOT EXISTS customer_vector_embedding_run_0 PARTITION OF customer_vector_embedding FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS customer_vector_embedding_default PARTITION OF customer_vector_embedding DEFAULT;

CREATE TABLE IF NOT EXISTS runs (
    run_id SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_customer_id ON customer_matching (customer_id);
CREATE INDEX IF NOT EXISTS idx_run_id ON customer_matching(run_id);
CREATE INDEX IF NOT EXISTS idx_customer_keys_binary_key ON customer_keys (binary_key);
CREATE INDEX IF NOT EXISTS idx_customer_tokens_ngram_token ON customer_tokens (ngram_token);
CREATE INDEX IF NOT EXISTS idx_tokens_idf_ngram_token ON tokens_idf (ngram_token);
CREATE INDEX IF NOT EXISTS idx_customer_vector_embedding_run_id ON customer_vector_embedding(run_id);
CREATE INDEX IF NOT EXISTS idx_customer_keys_run_id_binary_key ON customer_keys(run_id, binary_key);
CREATE INDEX IF NOT EXISTS idx_customer_tokens_run_id_ngram_token_entity_type_id ON customer_tokens(run_id, ngram_token, entity_type_id);
CREATE INDEX IF NOT EXISTS idx_customer_matching_run_id ON customer_matching(run_id);

-- Ensure sequence value for customer_id is correct
SELECT setval(pg_get_serial_sequence('customer_matching', 'customer_id'), COALESCE((SELECT MAX(customer_id) FROM customer_matching), 1), false);

-- Insert reference entities
INSERT INTO public.reference_entities (ID, entity_value)
VALUES
    (1, '9533 little forest'),
    (2, '4806 sunny forest heath'),
    (3, '4103 hidden pioneer gate'),
    (4, '1306 fallen mountain glade'),
    (5, '1534 cinder view thicket'),
    (6, '5103 burning embers green'),
    (7, '4565 quiet fox hill'),
    (8, '2909 gentle fawn round'),
    (9, '1221 rustic dale'),
    (10, '7910 bright grove stead')
ON CONFLICT (ID) DO NOTHING;

-- Create customers table if not exists
CREATE TABLE IF NOT EXISTS public.customers (
  customer_id INTEGER,
  customer_fname TEXT,
  customer_lname TEXT,
  customer_email TEXT,
  customer_password TEXT,
  customer_street TEXT,
  customer_city TEXT,
  customer_state TEXT,
  customer_zipcode TEXT,
  PRIMARY KEY (customer_id)
);

