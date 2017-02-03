# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json

import boto3
from moto import mock_s3
from moto import mock_sqs

import time


class Setup(object):
    """ Class to handle setting up AWS resources for testing

    """
    def __init__(self, region="us-east-1"):
        self.mock = True
        self.mock_s3 = None
        self.mock_sqs = None
        self.region = region

    def start_mocking(self):
        """Method to start mocking"""
        self.mock = True
        self.mock_s3 = mock_s3()
        self.mock_sqs = mock_sqs()
        self.mock_s3.start()
        self.mock_sqs.start()

    def stop_mocking(self):
        """Method to stop mocking"""
        self.mock_s3.stop()
        self.mock_sqs.stop()

    # ***** Bucket *****
    def _create_bucket(self, bucket_name):
        """Method to create the S3 bucket"""
        client = boto3.client('s3', region_name=self.region)
        _ = client.create_bucket(
            ACL='private',
            Bucket=bucket_name
        )
        return client.get_waiter('bucket_exists')

    def create_bucket(self, bucket_name):
        """Method to create the S3 bucket storage"""
        if self.mock:
            with mock_s3():
                self._create_bucket(bucket_name)
        else:
            waiter = self._create_bucket(bucket_name)

            # Wait for bucket to exist
            waiter.wait(Bucket=bucket_name)

    def _delete_bucket(self, bucket_name):
        """Method to delete the S3 bucket"""
        s3 = boto3.resource('s3', region_name=self.region)
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.all():
            obj.delete()

        # Delete bucket
        bucket.delete()
        return bucket

    def delete_bucket(self, bucket_name):
        """Method to create the S3 bucket"""
        if self.mock:
            with mock_s3():
                self._delete_bucket(bucket_name)
        else:
            bucket = self._delete_bucket(bucket_name)
            # Wait for table to be deleted (since this is real)
            bucket.wait_until_not_exists()
    # ***** END Bucket *****

    # ***** SQS Queue *****
    def _create_queue(self, queue_name):
        """Method to create a test sqs queue"""
        client = boto3.client('sqs', region_name=self.region)
        # Set big visibility timeout because nothing is deleting messages (no lambda running on unit tests)
        response = client.create_queue(QueueName=queue_name,
                                       Attributes={
                                         'VisibilityTimeout': '500',
                                         'DelaySeconds': '0',
                                         'MaximumMessageSize': '262144'
                                       })
        url = response['QueueUrl']
        return url

    def create_queue(self, queue_name):
        """Method to create a test sqs"""
        if self.mock:
            with mock_sqs():
                url = self._create_queue(queue_name)
        else:
            url = self._create_queue(queue_name)
            time.sleep(30)
        return url

    def _delete_queue(self, queue_url):
        """Method to delete a test sqs"""
        client = boto3.client('sqs', region_name=self.region)
        client.delete_queue(QueueUrl=queue_url)

    def delete_queue(self, queue_name):
        """Method to delete a test sqs"""
        if self.mock:
            with mock_sqs():
                self._delete_queue(queue_name)
        else:
            self._delete_queue(queue_name)
    # ***** END Flush SQS Queue *****

    def _add_tasks(self, id, secret, queue_url, backend_instance):
        """Push some fake tasks on the task queue"""
        client = boto3.client('sqs', region_name=self.region, aws_access_key_id=id,
                              aws_secret_access_key=secret)

        params = {"collection": 1,
                  "experiment": 2,
                  "channel": 3,
                  "resolution": 0,
                  "x_index": 5,
                  "y_index": 6,
                  "z_index": 1,
                  "t_index": 0,
                  "num_tiles": 16,
                  }

        self.test_msg = []
        for t_idx in range(0, 4):
            params["t_index"] = t_idx
            proj = [str(params['collection']), str(params['experiment']), str(params['channel'])]
            tile_key = backend_instance.encode_tile_key(proj,
                                                        params['resolution'],
                                                        params['x_index'],
                                                        params['y_index'],
                                                        params['z_index'],
                                                        params['t_index'],
                                                        )

            chunk_key = backend_instance.encode_chunk_key(params['num_tiles'], proj,
                                                          params['resolution'],
                                                          params['x_index'],
                                                          params['y_index'],
                                                          params['z_index'],
                                                          params['t_index'],
                                                          )

            msg = {"tile_key": tile_key, "chunk_key": chunk_key}
            client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))
            self.test_msg.append(msg)

    def add_tasks(self, id, secret, queue_url, backend_instance):
        """Push some fake tasks on the task queue"""
        if self.mock:
            mock_sqs(self._add_tasks(id, secret, queue_url, backend_instance))
        else:
            self._add_tasks(id, secret, queue_url, backend_instance)
