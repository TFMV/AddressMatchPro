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

CREATE TABLE customer_matching (
    customer_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    phone_number TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    run_id INT
);

CREATE INDEX idx_run_id ON customer_matching(run_id);

-- Create customer_keys table partitioned by run_id
CREATE TABLE customer_keys (
    customer_id INT,
    binary_key TEXT,
    run_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer_matching (customer_id)
) PARTITION BY LIST (run_id);

CREATE TABLE customer_keys_run_0 PARTITION OF customer_keys FOR VALUES IN (0);
CREATE TABLE customer_keys_default PARTITION OF customer_keys DEFAULT;

-- Create customer_tokens table partitioned by run_id
CREATE TABLE customer_tokens (
    customer_id INT,
    entity_type_id INT,
    ngram_token TEXT,
    ngram_tfidf FLOAT8,
    run_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer_matching (customer_id)
) PARTITION BY LIST (run_id);

CREATE TABLE customer_tokens_run_0 PARTITION OF customer_tokens FOR VALUES IN (0);
CREATE TABLE customer_tokens_default PARTITION OF customer_tokens DEFAULT;

-- Create tokens_idf table partitioned by run_id
CREATE TABLE tokens_idf (
    entity_type_id INT,
    ngram_token TEXT,
    ngram_idf FLOAT8,
    run_id INT NOT NULL
) PARTITION BY LIST (run_id);

CREATE TABLE tokens_idf_run_0 PARTITION OF tokens_idf FOR VALUES IN (0);
CREATE TABLE tokens_idf_default PARTITION OF tokens_idf DEFAULT;

-- Create customer_vector_embedding table partitioned by run_id
CREATE TABLE customer_vector_embedding (
    customer_id INT,
    vector_embedding VECTOR(300),
    run_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customer_matching (customer_id)
) PARTITION BY LIST (run_id);

CREATE TABLE customer_vector_embedding_run_0 PARTITION OF customer_vector_embedding FOR VALUES IN (0);
CREATE TABLE customer_vector_embedding_default PARTITION OF customer_vector_embedding DEFAULT;

-- Create indexes for performance
CREATE INDEX idx_customer_keys_binary_key ON customer_keys (binary_key);
CREATE INDEX idx_customer_tokens_ngram_token ON customer_tokens (ngram_token);
CREATE INDEX idx_tokens_idf_ngram_token ON tokens_idf (ngram_token);

CREATE TABLE runs (
    run_id SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


SELECT setval(pg_get_serial_sequence('customer_matching', 'customer_id'), COALESCE((SELECT MAX(customer_id) FROM customer_matching), 1), false);


 WITH embeddings AS (
    SELECT customer_id, vector_embedding
    FROM customer_vector_embedding
    WHERE run_id = 23
),
matching_embeddings AS (
    SELECT
        cv0.customer_id,
        cv0.vector_embedding,
        e.customer_id AS matched_customer_id,
        e.vector_embedding AS matched_vector_embedding,
        cv0.vector_embedding <=> e.vector_embedding AS similarity
    FROM
        customer_vector_embedding cv0
    JOIN
        embeddings e
    ON
        cv0.vector_embedding <=> e.vector_embedding <= 0.1 -- Adjusted threshold
    WHERE
        cv0.run_id = 0
),
matching_keys AS (
    SELECT
        ck0.customer_id,
        ck.customer_id AS matched_customer_id
    FROM
        customer_keys ck0
    JOIN
        customer_keys ck
    ON
        ck0.binary_key = ck.binary_key
    WHERE
        ck0.run_id = 0
        AND ck.run_id = 23
),
combined_matches AS (
    SELECT
        COALESCE(me.customer_id, mk.customer_id) AS customer_id,
        me.vector_embedding,
        COALESCE(me.matched_customer_id, mk.matched_customer_id) AS matched_customer_id,
        me.matched_vector_embedding,
        me.similarity
    FROM
        matching_embeddings me
    FULL OUTER JOIN
        matching_keys mk
    ON
        me.customer_id = mk.customer_id AND me.matched_customer_id = mk.matched_customer_id
),
ngram_sums AS (
    SELECT
        vt0.customer_id,
        SUM(vt0.ngram_tfidf) AS candidate_tfidf,
        SUM(vt.ngram_tfidf) AS matched_tfidf
    FROM
        customer_tokens vt0
    JOIN
        customer_tokens vt
    ON
        vt0.ngram_token = vt.ngram_token
        AND vt0.entity_type_id = vt.entity_type_id
    JOIN
        combined_matches cm
    ON
        vt0.customer_id = cm.customer_id
        AND vt.customer_id = cm.matched_customer_id
    WHERE
        vt0.run_id = 0
        AND vt.run_id = 23
    GROUP BY
        vt0.customer_id
)
SELECT
    cm.customer_id,
    cm.vector_embedding,
    cm.matched_customer_id,
    cm.matched_vector_embedding,
    cm.similarity,
    ns.candidate_tfidf,
    ns.matched_tfidf
FROM
    combined_matches cm
LEFT JOIN
    ngram_sums ns
ON
    cm.customer_id = ns.customer_id
WHERE
    cm.customer_id = 13
ORDER BY
    similarity DESC NULLS LAST;