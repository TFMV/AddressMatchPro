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

import spacy
import psycopg2
from psycopg2.extras import Json
import sys

# Check if run_id is provided as a command-line argument
if len(sys.argv) != 2:
    print("Usage: python generate_embeddings.py <run_id>")
    sys.exit(1)

run_id = int(sys.argv[1])

# Load spaCy model
nlp = spacy.load("en_core_web_md")

# Database connection
conn = psycopg2.connect(
    dbname="tfmv",
    user="postgres",
    password="your_dbpassword",
    host="localhost",
    port="5432",
)
cur = conn.cursor()

# Fetch customer data from customer_matching with the given run_id
cur.execute(
    "SELECT customer_id, first_name, last_name, street, city, state, zip_code FROM customer_matching WHERE run_id = %s",
    (run_id,),
)
customers = cur.fetchall()

# Process each customer and generate embeddings
for customer in customers:
    customer_id, first_name, last_name, street, city, state, zip_code = customer
    full_text = f"{first_name} {last_name} {street} {city} {state} {zip_code}"
    doc = nlp(full_text)
    vector_list = doc.vector.tolist()

    # Insert embeddings into customer_vector_embedding with the given run_id
    insert_query = """
        INSERT INTO customer_vector_embedding (customer_id, vector_embedding, run_id)
        VALUES (%s, %s, %s)
    """
    cur.execute(insert_query, (customer_id, Json(vector_list), run_id))

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()

