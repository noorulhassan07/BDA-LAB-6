import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient

# Create static DataFrame
data = [
    {"id": 1, "name": "Ali", "value": "Data Science"},
    {"id": 2, "name": "Noor", "value": "Big Data"},
    {"id": 3, "name": "Hassan", "value": "Machine Learning"},
    {"id": 4, "name": "Ayesha", "value": "Deep Learning"},
    {"id": 5, "name": "Fatima", "value": "AI Engineering"},
]
df = pd.DataFrame(data)
table = pa.Table.from_pandas(df)
pq.write_table(table, 'sample.parquet')

# Connect to HDFS
client = InsecureClient('http://namenode:9870', user='hdfs')

# ✅ Create target directory if not exists
try:
    client.makedirs('/user/hdfs')
    print("✅ /user/hdfs directory ensured.")
except Exception as e:
    print("⚠️ Directory might already exist:", e)

# Upload parquet file
with open('sample.parquet', 'rb') as f:
    client.write('/user/hdfs/sample.parquet', f, overwrite=True)
print("✅ Uploaded 'sample.parquet' to HDFS successfully!")
