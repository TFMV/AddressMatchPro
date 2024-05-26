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

# Step 4: Select reference entities
def select_reference_entity(group):
    for street in group['street']:
        if street in street_counts.index[:10]:
            return abbreviate_street_name(street)
    return abbreviate_street_name(group['street'].iloc[0])

reference_entities = street_clusters.groupby('cluster').apply(select_reference_entity).reset_index(drop=True)

print("Selected Reference Entities:")
print(reference_entities)

# Generate a single SQL INSERT statement
values = ", ".join([f"({index + 1}, '{street}')" for index, street in reference_entities.items()])
insert_statement = f"INSERT INTO public.reference_entities (ID, entity_value) VALUES {values};"

# Print the SQL statement
print(insert_statement)
