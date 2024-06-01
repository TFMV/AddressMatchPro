with matches as (select input.customer_id as input_customer_id,
                        input.run_id as input_run_id,
                        input.first_name as input_first_name,
                        input.last_name as input_last_name,
                        input.street as input_street,
                        input.city as input_city,
                        input.state as input_state,
                        input.zip_code as input_zip_code,
                        input.phone_number as input_phone_number,
                        candidates.customer_id as candidate_customer_id,
                        candidates.run_id as candidate_run_id,
                        candidates.first_name as candidate_first_name,
                        candidates.last_name as candidate_last_name,
                        candidates.street as candidate_street,
                        candidates.city as candidate_city,
                        candidates.state as candidate_state,
                        candidates.zip_code as candidate_zip_code,
                        candidates.phone_number as candidate_phone_number,
                        candidate_vec.vector_embedding <=> input_vec.vector_embedding AS similarity
                 from customer_matching candidates
                          join customer_matching input
                               on ((candidates.state = input.state OR
                                    candidates.zip_code = input.zip_code) and
                                   (candidates.zip_code = input.zip_code OR
                                    candidates.city = input.city OR
                                    candidates.phone_number = input.phone_number)
                                   )
                          join customer_vector_embedding candidate_vec
                               on (candidate_vec.customer_id = candidates.customer_id and
                                   candidate_vec.run_id = candidates.run_id)
                          join customer_vector_embedding input_vec
                               on (input_vec.customer_id = input.customer_id and
                                   input_vec.run_id = input.run_id)
                 where candidates.run_id = 0
                   and input.run_id = 97),
    bin_keys as (
        select candidate.customer_id as candidate_customer_id,
               candidate.run_id as candidate_run_id,
               input.customer_id as input_customer_id,
               input.run_id as input_run_id
        from customer_keys candidate
        join customer_keys input
        on (candidate.binary_key = input.binary_key)
        join matches
        on (candidate.customer_id = matches.candidate_customer_id and
            candidate.run_id = matches.candidate_run_id and
            input.customer_id = matches.input_customer_id and
            input.run_id = matches.input_run_id)
    )
select matches.input_customer_id,
       matches.candidate_customer_id,
       case when bin_keys.candidate_customer_id is null then false else true end as binary_key_match,
       matches.similarity,
       matches.input_first_name,
       matches.candidate_first_name,
       matches.input_last_name,
       matches.candidate_last_name,
       sum(input_tfidf.ngram_tfidf * candidate_tfidf.ngram_tfidf) as tfidf_score
from matches
left outer join bin_keys
on (matches.candidate_run_id = bin_keys.candidate_run_id and
    matches.candidate_customer_id = bin_keys.candidate_run_id and
    matches.input_run_id = bin_keys.input_run_id and
    matches.input_customer_id = bin_keys.input_customer_id)
join customer_tokens input_tfidf
on (input_tfidf.run_id = matches.input_run_id and
    input_tfidf.customer_id = matches.input_customer_id)
join customer_tokens candidate_tfidf
on (candidate_tfidf.run_id = matches.candidate_run_id and
    candidate_tfidf.customer_id = matches.candidate_customer_id and
    candidate_tfidf.entity_type_id = input_tfidf.entity_type_id and
   candidate_tfidf.ngram_token = input_tfidf.ngram_token)
where matches.similarity <= .1
group by matches.input_customer_id,
       matches.candidate_customer_id,
       case when bin_keys.candidate_customer_id is null then false else true end,
       matches.similarity,
       matches.input_first_name,
       matches.candidate_first_name,
       matches.input_last_name,
       matches.candidate_last_name
order by matches.input_customer_id, matches.similarity asc;