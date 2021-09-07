# Credential files

Place credential files here for the tests to use to test their connection to the manager server.

## AWS credentials

```ini
aws_access_key_id = AKIA***
aws_secret_access_key = dalk***
region_name = eu-west-2
```

## SSH credentials

uses `pyini` - a package that allows for infering types from the ini config.

```ini
hostname = ec2-xx.xx.xx.xx.eu-west-2.compute.amazonaws.com
root = /home/ubuntu/testing
username = ubuntu
privateKeyFilePath = ~/Documents/Encryption-keys/key.pem
autoAddMissingHost = True
(int) timeout = 5
```