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
import numpy as np
from psycopg2.extras import Json

# Load the spacy model
nlp = spacy.load("en_core_web_md")

# Connect to your PostgreSQL database
conn = psycopg2.connect(
    dbname="tfmv",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Query to select customer information
query = "SELECT customer_id, lower(customer_fname) || ' ' || lower(customer_lname) || ' ' || lower(customer_street) as customer_info FROM customers"
cur.execute(query)
rows = cur.fetchall()

# Prepare insert statement
insert_query = "INSERT INTO customer_vector_embedding (customer_id, vector_embedding) VALUES (%s, %s)"

for row in rows:
    customer_id, customer_info = row
    doc = nlp(customer_info)
    vector = doc.vector
    # Convert numpy array to list for JSON serialization
    vector_list = vector.tolist()

    # Insert into the database
    cur.execute(insert_query, (customer_id, Json(vector_list)))

# Commit the transaction
conn.commit()

# Close the database connection
cur.close()
conn.close()

print("Vector embeddings insertion completed successfully.")
