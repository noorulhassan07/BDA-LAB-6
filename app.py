import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient

data = []
for i in range(5):
    id_ = int(input(f"Enter id for record {i+1}: "))
    name = input(f"Enter name for record {i+1}: ")
    value = input(f"Enter value for record {i+1}: ")
    data.append({"id": id_, "name": name, "value": value})

df = pd.DataFrame(data)
table = pa.Table.from_pandas(df)

pq.write_table(table, 'sample.parquet')

client = InsecureClient('http://namenode:50070', user='hdfs')
with open('sample.parquet', 'rb') as f:
    client.write('/user/hdfs/sample.parquet', f, overwrite=True)
