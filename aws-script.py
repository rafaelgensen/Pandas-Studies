import boto3
import pandas as pd

# Initialize s3 client
s3 = boto3.client('s3')

# Define the bucket and object key
src_bucket_name = 'qtb-gluedemo-source-data'

src_file_key = 'getting-started/flights_source.csv'

# Download the file from s3
obj = s3.get_object(Bucket=src_bucket_name, Key=src_file_key)
df = pd.read_csv(obj['Body'])

# Perform a simple transformation (e.g., filter rows)
transformed_df = df[df['CANCELLED'] > 0]

tgt_bucket_name = 'qtb-gluedemo-target-data'
tgt_file_key = 'demo-script-python/output-file.csv'

# Write the transformed DataFrame back to s3
transformed_df.to_csv(f's3://{tgt_bucket_name}/{tgt_file_key}', index = False)