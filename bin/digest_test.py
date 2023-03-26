



import boto3
import stow
import logging
import zlib
import hashlib

stowLogger = logging.getLogger('stow')
stowLogger.addHandler(logging.StreamHandler())
stowLogger.setLevel(logging.DEBUG)

s3 = boto3.client('s3', region_name='eu-west-2')

digest = hashlib.sha256(b'Content').hexdigest()

print(digest)

hashlib.sha256()

# s3.put_object(
#             Bucket="kieran-bacon",
#             Key="file-1.txt",
#             Body=b"Content",
#             ChecksumAlgorithm="SHA256",
#             # ChecksumSHA256=digest
#         )

response = s3.head_object(
            Bucket="kieran-bacon",
            Key="file-1.txt",
)

print(response)

response = s3.get_object_attributes(Bucket="kieran-bacon", Key='file-1.txt', ObjectAttributes=['Checksum'])
print(response)

# stow.digest('s3://kieran-bacon/test-folder/readme.md', stow.HashingAlgorithm.CRC32)

