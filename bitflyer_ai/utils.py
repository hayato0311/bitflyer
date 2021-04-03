from manage import REF_LOCAL
import pandas as pd


if not REF_LOCAL:
    from aws import S3
    s3 = S3()


def path_exists(p_path):
    if REF_LOCAL:
        return p_path.exists()
    else:
        return s3.key_exists(str(p_path))


def read_csv(p_path):
    if REF_LOCAL:
        return pd.read_csv(p_path)
    else:
        return s3.read_csv(p_path)


def df_to_csv(path, df, index=True):
    if REF_LOCAL:
        return df.to_csv(path, index=index)
    else:
        return s3.to_csv(path, df, index=index)
