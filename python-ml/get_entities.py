import pandas as pd
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
import numpy as np

# Define the standardize_address function
def standardize_address(first_name, last_name, phone_number, street, city, state, zip_code):
    # Concatenate and standardize the address fields
    address = f"{first_name} {last_name} {phone_number} {street} {city} {state} {zip_code}"
    # Standardize the address (this function should match your Go implementation)
    standardized_address = address.upper().strip()
    return standardized_address

# Connect to the PostgreSQL database
engine = create_engine('postgresql://user:password@localhost:5432/mydb')

# Query the customer data
query = """
SELECT first_name, last_name, phone_number, street, city, state, zip_code
FROM customers
"""
customers = pd.read_sql(query, engine)

# Standardize and combine columns into a single text field
customers['standardized_entity'] = customers.apply(
    lambda row: standardize_address(
        row['first_name'], row['last_name'], row['phone_number'],
        row['street'], row['city'], row['state'], row['zip_code']
    ), axis=1
)
entities = customers['standardized_entity'].values

# Convert text data to TF-IDF vectors
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(entities)

# Perform PCA
pca = PCA(n_components=10)
X_pca = pca.fit_transform(X.toarray())

# Find the index of the representative entities
representative_indices = np.argsort(np.sum(np.abs(X_pca), axis=1))[-10:]
representative_entities = entities[representative_indices]

# Insert representative entities into the reference_entities table
reference_entities = pd.DataFrame({'entity_value': representative_entities})
reference_entities.to_sql('reference_entities', engine, if_exists='append', index=False)
