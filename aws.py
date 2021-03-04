import boto3
import pandas as pd
from io import StringIO


class S3:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

        self.s3_get = boto3.client('s3')

        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(self.bucket_name)

    def read_csv(self, object_key):
        # objkey = container_name + '/' + filename + '.csv'  # 多分普通のパス
        obj = self.s3_get.get_object(Bucket=self.bucket_name, Key=object_key)
        body = obj['Body'].read()
        bodystr = body.decode('utf-8')
        df = pd.read_csv(StringIO(bodystr))
        return df

    def to_csv(self, object_key, df, index=True):
        df_csv = df.to_csv(index=index)
        new_object = self.bucket.Object(object_key)
        new_object.put(Body=df_csv)
