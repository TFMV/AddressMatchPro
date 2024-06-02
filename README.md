# AddressMatchPro

![AddressMatchPro](assets/FuzzyMatchFinder.webp)

Welcome to AddressMatchPro! This project is currently under development, but we're excited to share our progress and goals with you. Stay tuned for updates!

## Project Overview

AddressMatchPro is an advanced entity matching solution leveraging machine learning to provide approximate matching. Our approach combines traditional algorithms with modern machine learning techniques to deliver high-accuracy matching results.

## Approach

### Data Ingestion and Preparation

- **Data Loading:** Load customer data from a provided dataset into a new table, `customer_matching`, in Postgres.
- **Data Partitioning:** Use `run_id` to manage different data loads and ensure efficient processing and querying.

### Core Matching Logic

- **Binary Key Generation:** Generate binary keys for addresses using reference entities and n-gram frequency similarity.
- **TF-IDF Calculation:** Calculate term frequency-inverse document frequency (TF-IDF) vectors for customer data to enhance matching precision.
- **Vector Embeddings:** Generate vector embeddings for customer data using spaCy, leveraging both address data and other attributes.

### API Development

- **Single Record Matching:** Develop an API endpoint to match a single record against the candidate space.
- **Batch Record Matching:** Develop an API endpoint to match multiple records provided in a CSV file against the candidate space.
- **Efficient Querying:** Utilize optimized SQL queries to retrieve potential matches based on binary keys or vector similarity.

### Integration with Machine Learning

- **ML Model Integration:** Integrate machine learning models to further enhance the accuracy of the matching process.
- **Scoring System:** Implement a scoring system to rank match candidates based on similarity scores.

## Major Goals and Milestones

### Phase 1: Initial Setup

- [x] Set up the project structure.
- [x] Establish database connection with Postgres using pgx/v5.
- [x] Implement basic API endpoints.
- [x] Create `customer_matching` table and load initial data.

### Phase 2: Core Matching Logic

- [x] Develop approximate matching algorithms.
- [x] Generate binary keys for customer addresses.
- [x] Implement n-gram frequency similarity for binary key generation.
- [x] Calculate and insert TF-IDF vectors.
- [x] Generate vector embeddings using Python and spaCy.
- [x] Support single match request use case
- [x] Support batch match requests
- [ ] Top Layer Logistic Regression Model (awaiting labeled examples)

### Phase 3: API Development

- [x] Create endpoints for matching entities.
- [x] Develop endpoint for single record matching.
- [x] Develop endpoint for batch record matching.
- [x] Implement middleware for request validation and logging.
- [x] Implement Fast CSV Loader
- [x] Develop utility functions for response formatting.

### Phase 4: Testing and Optimization

- [ ] Write unit and integration tests.
- [x] Optimize matching algorithms for performance.
- [x] Perform load testing and scalability improvements.

### Phase 5: Deployment

- [ ] Set up CI/CD pipeline.
- [ ] Deploy the API to Google Cloud Run.
- [ ] Monitor and maintain the service.

## Examples

### Request (POST)

```json
{
  "first_name": "mary",
  "last_name": "baldwin",
  "phone_number": "",
  "street": "7922 Iron Oak gardens",
  "city": "Caguas",
  "state": "PR",
  "zip_code": "00725",
  "top_n": 10
}
```

### Response

```json
[
  {
    "input_customer_id": 43,
    "input_run_id": 132,
    "input_first_name": "mary",
    "input_last_name": "baldwin",
    "input_street": "7922 iron oak gardens",
    "input_city": "caguas",
    "input_state": "pr",
    "input_zip_code": "00725",
    "input_phone_number": "",
    "candidate_customer_id": 13,
    "candidate_run_id": 0,
    "candidate_first_name": "mary",
    "candidate_last_name": "baldwin",
    "candidate_street": "7922 iron oak gardens",
    "candidate_city": "caguas",
    "candidate_state": "pr",
    "candidate_zip_code": "00725",
    "candidate_phone_number": "",
    "similarity": 0,
    "bin_key_match": true,
    "tfidf_score": 9.503990391442475,
    "rank": 1,
    "score": 100,
    "trigram_cosine_first_name": 1.0000000000000002,
    "trigram_cosine_last_name": 1.0000000000000002,
    "trigram_cosine_street": 1,
    "trigram_cosine_city": 0.9999999999999998,
    "trigram_cosine_phone_number": 1,
    "trigram_cosine_zip_code": 1
  },
  {
    "input_customer_id": 43,
    "input_run_id": 132,
    "input_first_name": "mary",
    "input_last_name": "baldwin",
    "input_street": "7922 iron oak gardens",
    "input_city": "caguas",
    "input_state": "pr",
    "input_zip_code": "00725",
    "input_phone_number": "",
    "candidate_customer_id": 6078,
    "candidate_run_id": 0,
    "candidate_first_name": "roger",
    "candidate_last_name": "clark",
    "candidate_street": "7922 iron oak gardens",
    "candidate_city": "caguas",
    "candidate_state": "pr",
    "candidate_zip_code": "00725",
    "candidate_phone_number": "",
    "similarity": 0.089910768646832,
    "bin_key_match": true,
    "tfidf_score": 1.0884455106047812,
    "rank": 2,
    "score": 28.96177170384219,
    "trigram_cosine_first_name": 0,
    "trigram_cosine_last_name": 0,
    "trigram_cosine_street": 1,
    "trigram_cosine_city": 0.9999999999999998,
    "trigram_cosine_phone_number": 1,
    "trigram_cosine_zip_code": 1
  },
  {
    "input_customer_id": 43,
    "input_run_id": 132,
    "input_first_name": "mary",
    "input_last_name": "baldwin",
    "input_street": "7922 iron oak gardens",
    "input_city": "caguas",
    "input_state": "pr",
    "input_zip_code": "00725",
    "input_phone_number": "",
    "candidate_customer_id": 1231,
    "candidate_run_id": 0,
    "candidate_first_name": "mary",
    "candidate_last_name": "norman",
    "candidate_street": "547 cinder oak glade",
    "candidate_city": "caguas",
    "candidate_state": "pr",
    "candidate_zip_code": "00725",
    "candidate_phone_number": "",
    "similarity": 0.09652949334468375,
    "bin_key_match": false,
    "tfidf_score": 0.3237762126857728,
    "rank": 3,
    "score": 14.197883721290156,
    "trigram_cosine_first_name": 1.0000000000000002,
    "trigram_cosine_last_name": 0,
    "trigram_cosine_street": 0.3429971702850177,
    "trigram_cosine_city": 0.9999999999999998,
    "trigram_cosine_phone_number": 1,
    "trigram_cosine_zip_code": 1
  },
  {
    "input_customer_id": 43,
    "input_run_id": 132,
    "input_first_name": "mary",
    "input_last_name": "baldwin",
    "input_street": "7922 iron oak gardens",
    "input_city": "caguas",
    "input_state": "pr",
    "input_zip_code": "00725",
    "input_phone_number": "",
    "candidate_customer_id": 5893,
    "candidate_run_id": 0,
    "candidate_first_name": "mary",
    "candidate_last_name": "young",
    "candidate_street": "9406 iron zephyr wood",
    "candidate_city": "caguas",
    "candidate_state": "pr",
    "candidate_zip_code": "00725",
    "candidate_phone_number": "",
    "similarity": 0.10975412256721129,
    "bin_key_match": false,
    "tfidf_score": 0.16866421762288136,
    "rank": 4,
    "score": 12.15361415783453,
    "trigram_cosine_first_name": 1.0000000000000002,
    "trigram_cosine_last_name": 0,
    "trigram_cosine_street": 0.17647058823529413,
    "trigram_cosine_city": 0.9999999999999998,
    "trigram_cosine_phone_number": 1,
    "trigram_cosine_zip_code": 1
  },
  {
    "input_customer_id": 43,
    "input_run_id": 132,
    "input_first_name": "mary",
    "input_last_name": "baldwin",
    "input_street": "7922 iron oak gardens",
    "input_city": "caguas",
    "input_state": "pr",
    "input_zip_code": "00725",
    "input_phone_number": "",
    "candidate_customer_id": 5817,
    "candidate_run_id": 0,
    "candidate_first_name": "mary",
    "candidate_last_name": "diaz",
    "candidate_street": "1795 little timber wood",
    "candidate_city": "caguas",
    "candidate_state": "pr",
    "candidate_zip_code": "00725",
    "candidate_phone_number": "",
    "similarity": 0.11930471441356283,
    "bin_key_match": false,
    "tfidf_score": 0.034527270214762346,
    "rank": 5,
    "score": 10.234922557380273,
    "trigram_cosine_first_name": 1.0000000000000002,
    "trigram_cosine_last_name": 0,
    "trigram_cosine_street": 0.05564148840746571,
    "trigram_cosine_city": 0.9999999999999998,
    "trigram_cosine_phone_number": 1,
    "trigram_cosine_zip_code": 1
  }
]
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Author

Thomas F McGeehan V
