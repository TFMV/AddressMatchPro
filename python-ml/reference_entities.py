# --------------------------------------------------------------------------------
# Author: Thomas F McGeehan V
#
# This file is part of a software project developed by Thomas F McGeehan V.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# For more information about the MIT License, please visit:
# https://opensource.org/licenses/MIT
#
# Acknowledgment appreciated but not required.
# --------------------------------------------------------------------------------

import pandas as pd
import psycopg2
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import pairwise_distances_argmin_min

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="tfmv",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)

# Query to extract street values from the database
query = "SELECT customer_street as street FROM public.customers"
df_customer = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Step 1: Extract unique street values
unique_streets = df_customer['street'].unique()

# Step 2: Frequency analysis
street_counts = df_customer['street'].value_counts()

# Step 3: Custom Tokenizer
def custom_tokenizer(text):
    return text.split()

vectorizer = TfidfVectorizer(tokenizer=custom_tokenizer, stop_words=None)
X = vectorizer.fit_transform(unique_streets)

# Clustering
num_clusters = 10
kmeans = KMeans(n_clusters=num_clusters, random_state=0).fit(X)

street_clusters = pd.DataFrame({'street': unique_streets, 'cluster': kmeans.labels_})

# Abbreviation mapping
abbreviation_map = {
    "avenue": "ave",
    "boulevard": "blvd",
    "circle": "cir",
    "court": "ct",
    "drive": "dr",
    "highway": "hwy",
    "lane": "ln",
    "place": "pl",
    "road": "rd",
    "street": "st",
    "terrace": "ter",
    "northwest": "nw",
    "southeast": "se",
    "southwest": "sw",
    "northeast": "ne",
    "unit": "unit",
    "ste": "ste",
    "apt": "apt",
    "floor": "fl",
    "po box": "pobox"
}

def abbreviate_street_name(street_name):
    tokens = street_name.lower().split()
    abbreviated_tokens = [abbreviation_map.get(token, token) for token in tokens]
    return ' '.join(abbreviated_tokens)

# Step 4: Find the street closest to the centroid of each cluster
centroids = kmeans.cluster_centers_
closest, _ = pairwise_distances_argmin_min(centroids, X)

# Select reference entities based on the closest streets to centroids
reference_entities = [abbreviate_street_name(unique_streets[index]) for index in closest]

print("Selected Reference Entities:")
print(reference_entities)

# Generate a single SQL INSERT statement
values = ", ".join([f"({index + 1}, '{street}')" for index, street in enumerate(reference_entities)])
insert_statement = f"INSERT INTO public.reference_entities (ID, entity_value) VALUES {values};"

# Print the SQL statement
print(insert_statement)

