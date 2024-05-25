import pandas as pd
import psycopg2
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="tfmv",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)

# Query to extract street values from the database
query = "SELECT street FROM public.customers"
df_customer = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Step 1: Extract unique street values
unique_streets = df_customer['street'].unique()

# Step 2: Frequency analysis
street_counts = df_customer['street'].value_counts()

# Step 3: Clustering
vectorizer = TfidfVectorizer(stop_words='english')
X = vectorizer.fit_transform(unique_streets)

num_clusters = 10
kmeans = KMeans(n_clusters=num_clusters, random_state=0).fit(X)

street_clusters = pd.DataFrame({'street': unique_streets, 'cluster': kmeans.labels_})

# Step 4: Select reference entities
reference_entities = street_clusters.groupby('cluster').apply(
    lambda x: x.loc[x['street'].isin(street_counts.index[:10]), 'street'].values[0]
).reset_index(drop=True)

print("Selected Reference Entities:")
print(reference_entities)
