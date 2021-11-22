import pyarrow.parquet as pq
table = pq.read_table('result.parquet')
print(table.to_pandas())