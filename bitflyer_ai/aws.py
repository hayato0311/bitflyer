from base64 import b64decode
from botocore.errorfactory import ClientError
import boto3
import pandas as pd
from io import StringIO
from manage import BUCKET_NAME
import os


class S3:
    def __init__(self):
        self.client = boto3.client('s3')

        resource = boto3.resource('s3')
        self.bucket = resource.Bucket(BUCKET_NAME)

    def read_csv(self, object_key):
        # objkey = container_name + '/' + filename + '.csv'  # 多分普通のパス
        obj = self.client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        body = obj['Body'].read()
        bodystr = body.decode('utf-8')
        df = pd.read_csv(StringIO(bodystr))
        return df

    def to_csv(self, object_key, df, index=True):
        df_csv = df.to_csv(index=index)
        new_object = self.bucket.Object(object_key)
        new_object.put(Body=df_csv)

    def key_exists(self, object_key):
        try:
            self.client.head_object(Bucket=BUCKET_NAME, Key=object_key)
            return True
        except ClientError:
            return False

    def listdir(self, object_key):
        if not object_key.endswith('/'):
            object_key += '/'
        result_tmp = self.client.list_objects(
            Bucket=BUCKET_NAME, Prefix=object_key, Delimiter='/'
        )
        result = [
            path['Prefix'] for path in result_tmp.get('CommonPrefixes', [])
        ]
        return result


def decrypt(encrypted):
    kms = boto3.client('kms')
    decrypted = boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted),
        EncryptionContext={
            'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']
        }
    )['Plaintext'].decode('utf-8')

    return decrypted
