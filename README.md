# AddressMatchPro

![AddressMatchPro](assets/FuzzyMatchFinder.webp)

## 🚧 Under Construction 🚧

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
- [x] Store and manage `run_id` for different data loads.
- [x] Clear old candidates from tables using `run_id`.

### Phase 3: API Development

- [x] Create endpoints for matching entities.
- [x] Develop endpoint for single record matching.
- [x] Develop endpoint for batch record matching.
- [x] Implement middleware for request validation and logging.
- [ ] Develop utility functions for response formatting.

### Phase 4: Testing and Optimization

- [ ] Write unit and integration tests.
- [ ] Optimize matching algorithms for performance.
- [ ] Perform load testing and scalability improvements.

### Phase 5: Deployment

- [ ] Set up CI/CD pipeline.
- [ ] Deploy the API to Google Cloud Run.
- [ ] Monitor and maintain the service.

## Get Involved

We welcome contributions and feedback from the community. Feel free to open issues or submit pull requests as we work towards our milestones. Let's make FuzzyMatchFinder the best entity matching solution together!

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Author

Thomas F McGeehan V
