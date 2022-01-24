import os
import pyini
import boto3
import uuid

from ... import ETC_DIR

CONFIG_PATH = os.path.join(ETC_DIR, 'aws_credentials.ini')
BUCKET_NAME_INCLUDE = 'pykb-storage-test-bucket'

def getBucketObjects():

    # Load in the config
    config = pyini.ConfigParser().read(CONFIG_PATH)

    # Set the environment for the tests
    os.environ["AWS_ACCESS_KEY_ID"] = config['aws_access_key_id']
    os.environ["AWS_SECRET_ACCESS_KEY"] = config['aws_secret_access_key']
    os.environ["AWS_DEFAULT_REGION"] = config['region_name']

    # Create the s3 resource
    s3 = boto3.resource(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'],
        region_name=config['region_name']
    )

    for bucket in s3.buckets.all():

        if BUCKET_NAME_INCLUDE in bucket.name:
            # Found a valid bucket for testing use
            bucket_name = bucket.name
            break

    else:
        # No bucket exists - create a new bucket

        counter = 0
        while True:
            bucket_name = '{}-{}'.format(uuid.uuid4(), BUCKET_NAME_INCLUDE)

            try:
                s3.create_bucket(Bucket=bucket_name)

            except s3.exceptions.ClientError as e:
                if counter < 2:
                    counter += 1
                    continue
                else:
                    raise RuntimeError('Failed to initialise a testing bucket')

            break

        bucket_name = bucket_name

    bucket = s3.Bucket(bucket_name)

    return s3, bucket, bucket_name
