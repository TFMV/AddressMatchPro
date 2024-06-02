WITH matches AS (
    SELECT 
        input.customer_id AS input_customer_id,
        input.run_id AS input_run_id,
        input.first_name AS input_first_name,
        input.last_name AS input_last_name,
        input.street AS input_street,
        input.city AS input_city,
        input.state AS input_state,
        input.zip_code AS input_zip_code,
        input.phone_number AS input_phone_number,
        candidates.customer_id AS candidate_customer_id,
        candidates.run_id AS candidate_run_id,
        candidates.first_name AS candidate_first_name,
        candidates.last_name AS candidate_last_name,
        candidates.street AS candidate_street,
        candidates.city AS candidate_city,
        candidates.state AS candidate_state,
        candidates.zip_code AS candidate_zip_code,
        candidates.phone_number AS candidate_phone_number,
        candidate_vec.vector_embedding <=> input_vec.vector_embedding AS similarity
    FROM customer_matching candidates
    JOIN customer_matching input
        ON ((candidates.state = input.state OR candidates.zip_code = input.zip_code) 
            AND (candidates.zip_code = input.zip_code OR candidates.city = input.city OR candidates.phone_number = input.phone_number))
    JOIN customer_vector_embedding candidate_vec
        ON (candidate_vec.customer_id = candidates.customer_id AND candidate_vec.run_id = candidates.run_id)
    JOIN customer_vector_embedding input_vec
        ON (input_vec.customer_id = input.customer_id AND input_vec.run_id = input.run_id)
    WHERE candidates.run_id = 0
    AND input.run_id = $1
),
bin_keys AS (
    SELECT 
        input.customer_id AS input_customer_id,
        match.customer_id AS match_customer_id
    FROM customer_keys input
    JOIN customer_keys match
        ON (input.binary_key = match.binary_key)
    JOIN matches
        ON (matches.input_customer_id = input.customer_id
            AND matches.candidate_customer_id = match.customer_id)
)
SELECT 
    COALESCE(matches.input_customer_id, 0) AS input_customer_id,
    COALESCE(matches.input_run_id, 0) AS input_run_id,
    COALESCE(matches.input_first_name, '') AS input_first_name,
    COALESCE(matches.input_last_name, '') AS input_last_name,
    COALESCE(matches.input_street, '') AS input_street,
    COALESCE(matches.input_city, '') AS input_city,
    COALESCE(matches.input_state, '') AS input_state,
    COALESCE(matches.input_zip_code, '') AS input_zip_code,
    COALESCE(matches.input_phone_number, '') AS input_phone_number,
    COALESCE(matches.candidate_customer_id, 0) AS candidate_customer_id,
    COALESCE(matches.candidate_run_id, 0) AS candidate_run_id,
    COALESCE(matches.candidate_first_name, '') AS candidate_first_name,
    COALESCE(matches.candidate_last_name, '') AS candidate_last_name,
    COALESCE(matches.candidate_street, '') AS candidate_street,
    COALESCE(matches.candidate_city, '') AS candidate_city,
    COALESCE(matches.candidate_state, '') AS candidate_state,
    COALESCE(matches.candidate_zip_code, '') AS candidate_zip_code,
    COALESCE(matches.candidate_phone_number, '') AS candidate_phone_number,
    COALESCE(matches.similarity, 1) AS similarity,
    CASE WHEN bin_keys.match_customer_id IS NULL THEN FALSE ELSE TRUE END AS bin_key_match,
    SUM(COALESCE(input_tfidf.ngram_tfidf, 0) * COALESCE(candidate_tfidf.ngram_tfidf, 0)) AS tfidf_score,
    RANK() OVER (PARTITION BY matches.input_customer_id ORDER BY matches.similarity) AS rank
FROM matches
JOIN customer_tokens input_tfidf
    ON (input_tfidf.run_id = matches.input_run_id 
        AND input_tfidf.customer_id = matches.input_customer_id)
JOIN customer_tokens candidate_tfidf
    ON (candidate_tfidf.run_id = matches.candidate_run_id 
        AND candidate_tfidf.customer_id = matches.candidate_customer_id 
        AND candidate_tfidf.entity_type_id = input_tfidf.entity_type_id 
        AND candidate_tfidf.ngram_token = input_tfidf.ngram_token)
LEFT OUTER JOIN bin_keys
    ON (bin_keys.input_customer_id = matches.input_customer_id 
        AND bin_keys.match_customer_id = matches.candidate_customer_id)
WHERE matches.similarity <= 0.12
GROUP BY matches.input_customer_id,
         matches.input_run_id,
         matches.input_first_name,
         matches.input_last_name,
         matches.input_street,
         matches.input_city,
         matches.input_state,
         matches.input_zip_code,
         matches.input_phone_number,
         matches.candidate_customer_id,
         matches.candidate_run_id,
         matches.candidate_first_name,
         matches.candidate_last_name,
         matches.candidate_street,
         matches.candidate_city,
         matches.candidate_state,
         matches.candidate_zip_code,
         matches.candidate_phone_number,
         CASE WHEN bin_keys.match_customer_id IS NULL THEN FALSE ELSE TRUE END,
         matches.similarity
ORDER BY matches.input_customer_id, matches.similarity;
