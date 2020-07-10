#!/usr/local/bin/python3
from optparse import OptionParser
import boto3
import logging
import os
import progressbar
import sys
import traceback

parser = OptionParser()
parser.add_option("-b", "--bucket", dest="bucket", default=os.environ.get('BUCKET'),
                  help="S3 bucket name", metavar="BUCKET")
(options, args) = parser.parse_args()
if not options.bucket:
    parser.error('Bucket not given')
bucket=options.bucket

client = boto3.client('s3')

try:
    widgets = ['Working: ', progressbar.AnimatedMarker(markers='◐◓◑◒')]
    bar = progressbar.ProgressBar(widgets=widgets)
except UnicodeError:
    sys.stdout.write('Unicode error: skipping example')

def get_s3_keys_as_generator(bucket):
    """Generate all the keys in an S3 bucket."""
    kwargs = {'Bucket': bucket}
    while True:
        resp = client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            yield obj['Key']
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

for key in get_s3_keys_as_generator(bucket=bucket):
    bar.update()
    try:
      replication_status = client.get_object(Bucket=bucket,Key=key)['ReplicationStatus']
    except KeyError as e:
        print(key + ': No status')
        continue
    except Exception as e:
        print(key + ': cought exception!')
        logging.error(traceback.format_exc())

    if (replication_status == "FAILED" ):
        print(key + ': Failed status')
        client.copy_object(Bucket=bucket, CopySource=bucket+'/'+key, Key=key)
