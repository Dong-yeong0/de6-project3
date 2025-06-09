from typing import BinaryIO, Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(
    key: str,
    bucket_name: str,
    data: Union[str, bytes, BinaryIO],
    aws_conn_id: str = 'aws_s3_conn',
    replace: bool = True
) -> None:
    ''' Upload file/data to S3'''
    hook = S3Hook(aws_conn_id=aws_conn_id)
    if isinstance(data, str):
        hook.load_string(
            string_data=data,
            key=key,
            bucket_name=bucket_name,
            replace=replace
        )
    else:
        hook.load_bytes(
            bytes_data=data,
            key=key,
            bucket_name=bucket_name,
            replace=replace
        )

def read_from_s3(
    key: str,
    bucket_name: str,
    aws_conn_id: str = 'aws_s3_conn'
):
    ''' Read data from S3 '''
    hook = S3Hook(aws_conn_id=aws_conn_id)
    return hook.read_key(key=key, bucket_name=bucket_name)