import stow
import pyini
import boto3

# BUCKET_NAME_INCLUDE = 'pykb-storage-test-bucket'

# config = pyini.ConfigParser().read(
#     stow.join(stow.dirname(stow.dirname(__file__)), "etc", "aws_credentials.ini")
# )

# print(config)

# _s3Resource = boto3.resource(
#     's3',
#     aws_access_key_id=config["aws_access_key_id"],
#     aws_secret_access_key=config["aws_secret_access_key"],
#     region_name="eu-west-2"
# )

# _s3Resource = boto3.resource(
#     's3',
#     aws_access_key_id=None,
#     aws_secret_access_key=None,
#     region_name=None,
# )

# for bucket in _s3Resource.buckets.all():

#     if BUCKET_NAME_INCLUDE in bucket.name:
#         # Found a valid bucket for testing use
#         bucket_name = bucket.name
#         break


# # Create a reference to the AWS bucket - create a Directory to represent it
# _bucket = _s3Resource.Bucket(name="ngenius-experiments") # pylint: disable=no-member

# _bucket.put_object(Key="here/test", Body=b'really this works')

# _bucket.put_object(Key="here", Body=b'and this is different')

import os
import shutil

# os.makedirs(os.path.dirname(dest_remote), exist_ok=True)
shutil.copy("file", "folder")


stow.put(x, y, merge=True)
stow.put(x, y, overwrite=True)