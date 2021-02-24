# Copyright 2019 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import six
from abc import ABCMeta, abstractmethod
import requests
import json
import boto3
from enum import Enum
import hashlib
import configparser
import time
import botocore
import os
import random

from ..utils import WaitPrinter
from ..utils.log import always_log_info
from ..utils.backoff import get_wait_time


class IngestStatus(Enum):
    """Return values used by upload()."""
    STOP = 0            # Stop ingest.
    UPLOAD = 1          # Continue uploading.
    WAIT = 2            # Wait on the backend to finish something.
    COMPLETING = 3      # Start polling job status while the backend verifies job.


class BackendStatus(Enum):
    """This enum mirrors boss.git/django/bossingest/models.py"""
    PREPARING = 0
    UPLOADING = 1
    COMPLETE = 2
    DELETED = 3
    FAILED = 4
    COMPLETING = 5
    WAIT_ON_QUEUES = 6

def convert_backend_to_ingest_status(status):
    """
    Convert a BackendStatus value to equivalent IngestStatus value for use by
    client.py.

    Args:
        status (BackendStatus)

    Returns:
        (IngestStatus)

    Raises:
        (ValueError): if not given a known BackendStatus value.
    """
    if status in [BackendStatus.PREPARING, BackendStatus.UPLOADING]:
        return IngestStatus.UPLOAD
    if status in [BackendStatus.COMPLETE, BackendStatus.DELETED, BackendStatus.FAILED]:
        return IngestStatus.STOP
    if status in [BackendStatus.COMPLETING, BackendStatus.WAIT_ON_QUEUES]:
        return IngestStatus.WAIT

    raise ValueError('Unknown status: {}'.format(status))


@six.add_metaclass(ABCMeta)
class Backend(object):
    """

    Attributes:
        config (dict):
        sqs:
        s3 (boto3.S3)::
        upload_queue (boto3.SQS.Queue): Queue that holds upload tile messages.
        tile_index_queue (boto3.SQS.Queue): Queue that triggers a tile index update.
        bucket (S3.Bucket): Tile bucket.
        volumetric_bucket (S3.Bucket): Temporary cuboid holding bucket for volumetric ingests.
    """
    def __init__(self, config):
        """
        A class to implement a backend that supports the ingest service

        Args:
            config (dict): Dictionary of parameters from the "backend" section of the config file

        """
        self.config = config
        self.sqs = None
        self.upload_queue = None
        self.tile_index_queue = None
        self.s3 = None
        self.bucket = None
        self.volumetric_bucket = None

    @abstractmethod
    def setup(self):
        """
        Method to configure the backend based on configuration parameters in the config file

        Args:

        Returns:
            None


        """
        return NotImplemented

    @abstractmethod
    def create(self, data):
        """
        Method to upload the config data to the backend to create an ingest job

        Args:
            data(dict): A dictionary of configuration parameters

        Returns:
            (int): The returned ingest_job_id


        """
        return NotImplemented

    @abstractmethod
    def join(self, ingest_job_id):
        """
        Method to join an ingest job upload

        Job Status: {0: Preparing, 1: Uploading, 2: Complete, 3: Deleted}

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (int, dict, str, str, str, dict, int): The job status, AWS credentials, and SQS upload_job_queue,
                                              tile_index_queue, tile bucket name, config_params to pass along
                                              during upload via metadata, and tile count
        """
        return NotImplemented

    @abstractmethod
    def cancel(self, ingest_job_id):
        """
        Method to cancel an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to cancel

        Returns:
            None


        """
        return NotImplemented

    @abstractmethod
    def complete(self, ingest_job_id):
        """
        Method to complete an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to complete

        Returns:
            (IngestStatus, int | None)): Status plus number of seconds to wait if IngestStatus.WAIT


        """
        return NotImplemented

    @abstractmethod
    def get_job_status(self, ingest_job_id):
        """
        Method to get the ingest job status

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (dict): Job status dictionary, including the number of messages in the queue


        """
        return NotImplemented

    @abstractmethod
    def get_task(self):
        """
        Method to get an upload task

        Args:

        Returns:
            None


        """
        return NotImplemented

    def setup_queues(self, credentials, upload_queue, tile_index_queue, region="us-east-1"):
        """
        Method to create a connection to the upload task queue

        Args:
            credentials(dict): AWS credentials
            upload_queue(str): The URL for the upload SQS queue
            tile_index_queue(str): URL of tile index queue
            region(str): The AWS region where the SQS queue exists

        Returns:
            None

        """
        self.sqs = boto3.resource('sqs', region_name=region, aws_access_key_id=credentials["access_key"],
                                  aws_secret_access_key=credentials["secret_key"])
        self.upload_queue = self.sqs.Queue(url=upload_queue)
        if tile_index_queue:
            self.tile_index_queue = self.sqs.Queue(url=tile_index_queue)

    def setup_tile_bucket(self, credentials, tile_bucket, region="us-east-1"):
        """
        Method to create a connection to the tile bucket

        Args:
            credentials(dict): AWS credentials
            tile_bucket(str): The name of the bucket
            region(str): The AWS region where the bucket exists

        Returns:
            None

        """
        self.s3 = boto3.resource('s3', region_name=region, aws_access_key_id=credentials["access_key"],
                                 aws_secret_access_key=credentials["secret_key"])
        self.bucket = self.s3.Bucket(tile_bucket)

    def setup_volumetric_bucket(self, credentials, bucket_name, region="us-east-1"):
        """
        Method to create a connection to the tile bucket

        Args:
            credentials(dict): AWS credentials
            bucket_name(str): The name of the bucket
            region(str): The AWS region where the bucket exists

        Returns:
            None

        """
        self.s3 = boto3.resource('s3', region_name=region, aws_access_key_id=credentials["access_key"],
                                 aws_secret_access_key=credentials["secret_key"])
        self.volumetric_bucket = self.s3.Bucket(bucket_name)

    @abstractmethod
    def encode_tile_key(self, project_info, resolution, x_index, y_index, z_index, t_index=0):
        """A method to create a tile key.

        The tile key is the key used for each individual tile file.

        Args:
            project_info(list): A list of strings containing the project/data model information for where data belongs
            resolution(int): The level of the resolution hierarchy.  Typically 0
            x_index(int): The x tile index
            y_index(int): The y tile index
            z_index(int): The z tile index
            t_index(int): The time index

        Returns:
            (str): The object key to use for uploading to the tile bucket
        """
        return NotImplemented

    @abstractmethod
    def encode_chunk_key(self, num_tiles, project_info, resolution, x_index, y_index, z_index, t_index=0):
        """A method to create a chunk key.

        A "chunk" is the group of tiles that must be uploaded so a cuboid can be ingested.  The chunk key is used
        to track all tiles in a given group.

        Args:
            num_tiles(int): The expected number of tiles in this chunk (in the z-dimension). Useful for forcing ingest of partial cuboids
            project_info(list): A list of strings containing the project/data model information for where data belongs
            resolution(int): The level of the resolution hierarchy.  Typically 0
            x_index(int): The x tile index
            y_index(int): The y tile index
            z_index(int): The z tile index
            t_index(int): The time index

        Returns:
            (str): The object key to use for uploading to the tile bucket
        """
        return NotImplemented

    @abstractmethod
    def decode_tile_key(self, key):
        """A method to decode the tile key

        The tile key is the key used for each individual tile file.

        Args:
            key(str): The key to decode

        Returns:
            (dict): A dictionary containing the components of the key
        """
        return NotImplemented

    @abstractmethod
    def decode_chunk_key(self, key):
        """A method to decode the chunk key

        The tile key is the key used for each individual tile file.

        Args:
            key(str): The key to decode

        Returns:
            (dict): A dictionary containing the components of the key
        """
        return NotImplemented

    @staticmethod
    def factory(backend_str, config_data):
        """
        Method to return a validator class based on a string
        Args:
            backend_str (str): String of the classname

        Returns:

        """
        if backend_str == "BossBackend":
            return BossBackend(config_data)
        else:
            return ValueError("Unsupported Backend: {}".format(backend_str))


class BossBackend(Backend):
    def __init__(self, config):
        """
        A class to implement a backend that supports the ingest service

        api_token is a dictionary with keys:

        Args:

        """
        self.host = None
        self.api_headers = None
        Backend.__init__(self, config)
        self.api_version = "latest"
        self.validate_ssl = True
        self.credential_timeout = 3300  # Currently credentials expire in 1 hr, so renew after 55 minutes

    def setup(self, api_token=None):
        """
        Method to configure the backend based on configuration parameters in the config file

        Args:

        Returns:
            None


        """
        self.host = "{}://{}".format(self.config["client"]["backend"]["protocol"],
                                     self.config["client"]["backend"]["host"])

        # If API token not provided, load API credentials from intern locations as needed.
        if not api_token:
            # Try environment var
            if "INTERN_TOKEN" in os.environ:
                api_token = os.environ["INTERN_TOKEN"]
            else:
                # Try to see if intern config file is setup
                try:
                    cfg_parser = configparser.ConfigParser()
                    cfg_parser.read(os.path.expanduser("~/.intern/intern.cfg"))
                    if "Default" in cfg_parser.sections():
                        api_token = cfg_parser.get("Default", "token")
                    elif "Project Service" in cfg_parser.sections():
                        api_token = cfg_parser.get("Project Service", "token")
                    else:
                        raise ValueError("Could not load config from ~/.intern/intern.cfg")
                except KeyError as e:
                    print("API Token not provided. Failed to setup backend: {}".format(e))

        self.api_headers = {'Authorization': 'Token ' + api_token, 'Accept': 'application/json',
                            'content-type': 'application/json'}

    def create(self, config_dict):
        """
        Method to upload the config data to the backend to create an ingest job

        Args:
            config_dict(dict): config data

        Returns:
            (int): The returned ingest_job_id


        """
        always_log_info("Submitting ingest job configuration for creation...")
        r = requests.post('{}/{}/ingest/'.format(self.host, self.api_version), json=config_dict,
                          headers=self.api_headers, verify=self.validate_ssl)

        if r.status_code != 201:
            msg = r.json()
            err_detail = None
            if "detail" in msg:
                err_detail = msg["detail"]
            elif "message" in msg:
                err_detail = msg["message"]

            raise Exception("Failed to create ingest job. Server side validation of configuration file failed: {}".format(err_detail))
        else:
            return r.json()['id']

    def join(self, ingest_job_id):
        """
        Method to join an ingest job upload

        Job Status: {0: Preparing, 1: Uploading, 2: Complete, 3: Deleted}

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (int, dict, str, str, str, dict, int): The job status, AWS credentials, and SQS upload_job_queue,
                                              tile_index_queue, tile bucket name, config_params to pass along
                                              during upload via metadata, and tile count
        """

        wp = WaitPrinter()
        maximum_retries = 1000
        retries = 0
        max_pause_in_ms = 1000000
        while True:
            r = requests.get('{}/{}/ingest/{}'.format(self.host, self.api_version, ingest_job_id),
                             headers=self.api_headers, verify=self.validate_ssl)
            if r.status_code in [400, 500, 502, 503]:
                retries += 1
                if retries > maximum_retries:
                    raise Exception("After {} attempts, failed to join ingest job: {}".format(maximum_retries, r.text))
                exp_backoff = 100 * 2 ** (retries + 4)  # in ms
                pause_for = random.uniform(1, min(max_pause_in_ms, exp_backoff)) / 1000  # in secs
                if r.status_code == 400:
                    print(r.text)   # Can see if rate is being exceeded and if any other 400s are occuring.
                print("Join request failed with code: {}, retry attempt: {}, pausing for {:0.3f} seconds before retrying.".format(r.status_code, retries, pause_for))
                time.sleep(pause_for)
            elif r.status_code != 200:
                raise Exception("Failed to join ingest job after {} retry attempts: {}".format(retries, r.text))
            else:
                result = r.json()
                job_status = int(result['ingest_job']["status"])
                wp.print_msg("(pid={}) Waiting for ingest job to be created".format(os.getpid()))
                if job_status == 0:
                    time.sleep(5)
                else:
                    wp.finished()
                    creds = result["credentials"]

                    # Add check to make sure valid credentials came back. If not, try again
                    if not creds:
                        continue

                    upload_queue = result["ingest_job"]["upload_queue"]
                    tile_index_queue = result["ingest_job"]["tile_index_queue"]
                    tile_bucket = result["tile_bucket_name"]
                    num_tiles = result["ingest_job"]["tile_count"]

                    # Setup params for the rest of the ingest process
                    # These params are passed as metadata that accompanies the
                    # S3 object.
                    params = {}
                    params["upload_queue"] = result["ingest_job"]["upload_queue"]
                    params["ingest_queue"] = result["ingest_job"]["ingest_queue"]
                    params["ingest_lambda"] = result["ingest_lambda"]
                    params["KVIO_SETTINGS"] = result["KVIO_SETTINGS"]
                    params["STATEIO_CONFIG"] = result["STATEIO_CONFIG"]
                    params["OBJECTIO_CONFIG"] = result["OBJECTIO_CONFIG"]
                    params["resource"] = result["resource"]

                    self.setup_queues(creds, upload_queue, tile_index_queue, region="us-east-1")
                    self.setup_tile_bucket(creds, tile_bucket, region="us-east-1")
                    if "ingest_bucket_name" in result:
                        self.setup_volumetric_bucket(creds, result["ingest_bucket_name"], region="us-east-1")

                    return job_status, creds, upload_queue, tile_index_queue, tile_bucket, params, num_tiles

    def cancel(self, ingest_job_id):
        """
        Method to cancel an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            None


        """
        r = requests.delete('{}/{}/ingest/{}'.format(self.host, self.api_version, ingest_job_id),
                            headers=self.api_headers, verify=self.validate_ssl)

        if r.status_code != 204:
            raise Exception("Failed to cancel ingest job: {}".format(r.json()))

    def complete(self, ingest_job_id):
        """
        Method to complete an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to complete

        Returns:
            (IngestStatus, int): Status and number of secs to keep waiting


        """
        r = requests.post('{}/{}/ingest/{}/complete'.format(self.host, self.api_version, ingest_job_id),
                          headers=self.api_headers, verify=self.validate_ssl)

        if r.status_code == 204:
            return IngestStatus.STOP, 0

        try:
            data = r.json()
        except Exception as ex:
            raise Exception("Error parsing response from backend: {}".format(ex))

        if r.status_code == 400:
            if 'wait_secs' in data:
                return IngestStatus.WAIT, data['wait_secs']

        if r.status_code == 202:
            if 'job_status' in data:
                status = BackendStatus(data['job_status'])
                if status == BackendStatus.WAIT_ON_QUEUES:
                    if 'wait_secs' in data:
                        return IngestStatus.WAIT, data['wait_secs']
                elif status == BackendStatus.COMPLETING:
                    return IngestStatus.COMPLETING, 0

        raise Exception("Failed to complete ingest job: {}".format(data))

    def get_task(self, num_messages=1):
        """
        Method to get an upload task

        Args:
            num_messages(int): Number of messages to pop off the upload task queue

        Returns:
            (str, str, dict): message_id, receipt_handle, message contents
        """
        try_cnt = 0
        msg = None
        while try_cnt < 19:
            try:
                msg = self.upload_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=1)
                break
            except botocore.exceptions.ClientError as e:
                print("(pid={}) Waiting for credentials to be valid".format(os.getpid()))
                try_cnt += 1
                time.sleep(15)

                if try_cnt >= 20:
                    raise Exception("(pid={}) Credentials failed to be come valid".format(os.getpid()))

        if msg:
            return msg[0].message_id, msg[0].receipt_handle, json.loads(msg[0].body)
        else:
            return None, None, None

    def delete_task(self, msg_id, receipt_handle):
        """
        Delete a message from the upload queue

        Args:
            msg_id (str): Id of queue message.
            receipt_handle (str): Actual id required to delete the message.

        Returns:
            (bool): True on success.

        Raises:
            (Exception): Raised after n consecutive ClientErrors.
        """
        MAX_TRIES = 20
        try_cnt = 0
        while try_cnt < MAX_TRIES - 1:
            try:
                resp = self.upload_queue.delete_messages(Entries=[{'Id': msg_id, 'ReceiptHandle': receipt_handle}])
                if 'Successful' in resp and len(resp['Successful']) > 0:
                    if resp['Successful'][0]['Id'] == msg_id:
                        return True

                # Failed for some reason.
                try_cnt += 1
                if 'Failed' in resp and len(resp['Failed']) > 0:
                    err = resp['Failed'][0]
                    always_log_info('Failed deleting message from queue: ({}) - {}'.format(err['Code'], err['Message']))
                    if err['SenderFault']:
                        # If it's our fault, give up.
                        break

                time.sleep(get_wait_time(try_cnt))
            except botocore.exceptions.ClientError:
                print("(pid={}) Waiting for credentials to be valid".format(os.getpid()))
                try_cnt += 1
                time.sleep(15)

                if try_cnt >= MAX_TRIES:
                    raise Exception("(pid={}) Credentials failed to be come valid".format(os.getpid()))

        return False

    def put_task(self, msg, max_retries):
        """
        Place the message from the upload queue on the tile index queue so the
        tile index is updated.

        Args:
            msg (str): Message contents.
            max_retries (int): Max number retries to put message on queue.

        Returns:
            (bool): False on failure.
        """
        try_cnt = 0
        while try_cnt < max_retries + 1:
            try:
                self.tile_index_queue.send_message(MessageBody=msg)
                return True
            except botocore.exceptions.ClientError:
                try_cnt += 1
                time.sleep(get_wait_time(try_cnt))

        return False

    def get_job_status(self, ingest_job_id):
        """
        Method to get the job status

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (int)
        """

        maximum_retries = 100
        retries = 0
        while True:
            r = requests.get('{}/{}/ingest/{}/status'.format(self.host, self.api_version, ingest_job_id),
                             headers=self.api_headers, verify=self.validate_ssl)
            if r.status_code in [500, 502, 503]:
                retries += 1
                if retries > maximum_retries:
                    raise Exception("After {} attempts, failed to join ingest job: {}".format(maximum_retries, r.text))

                exp_backoff = 100 * 2 ** retries  # in ms
                pause_for = random.uniform(1, min(30000, exp_backoff)) / 1000
                print("Job status request failed with: {} pausing for {:0.3f} seconds before retrying.".format(r.status_code,
                                                                                                         pause_for))
                time.sleep(pause_for)

            elif r.status_code != 200:
                raise Exception("Failed to get ingest job status: {}".format(r.text))
            else:
                return r.json()

    def encode_tile_key(self, project_info, resolution, x_index, y_index, z_index, t_index=0):
        """A method to create a tile key.

        The tile key is the key used for each individual tile file.

        Args:
            project_info(list): A list of strings containing the project/data model information for where data belongs
            resolution(int): The level of the resolution hierarchy.  Typically 0
            x_index(int): The x tile index
            y_index(int): The y tile index
            z_index(int): The z tile index
            t_index(int): The time index

        Returns:
            (str): The object key to use for uploading to the tile bucket
        """
        proj_str = six.u("&".join([str(x) for x in project_info]))
        base_key = six.u("{}&{}&{}&{}&{}&{}".format(proj_str, resolution, x_index, y_index, z_index, t_index))

        hashm = hashlib.md5()
        hashm.update(base_key.encode())

        return six.u("{}&{}".format(hashm.hexdigest(), base_key))

    def encode_chunk_key(self, num_tiles, project_info, resolution, x_index, y_index, z_index, t_index=0):
        """A method to create a chunk key.

        A "chunk" is either a single 3D volume or a group of tiles that must be uploaded so cuboids can be ingested.  The chunk key
        identifies the 3D volume or all tiles in a given group.

        Args:
            num_tiles(int): The expected number of tiles in this chunk (in the z-dimension). Useful for forcing ingest of partial cuboids.  For a 3D volume, its value is 1.
            project_info(list): A list of strings containing the project/data model information for where data belongs
            resolution(int): The level of the resolution hierarchy.  Typically 0
            x_index(int): The x tile index
            y_index(int): The y tile index
            z_index(int): The z tile index
            t_index(int): The time index

        Returns:
            (str): The object key to use for uploading to the tile bucket
        """
        proj_str = six.u("&".join([str(x) for x in project_info]))
        base_key = six.u("{}&{}&{}&{}&{}&{}&{}".format(num_tiles, proj_str,
                                                       resolution, x_index, y_index, z_index, t_index))

        hashm = hashlib.md5()
        hashm.update(base_key.encode())

        return six.u("{}&{}".format(hashm.hexdigest(), base_key))

    def decode_tile_key(self, key):
        """A method to decode the tile key

        The tile key is the key used for each individual tile file.

        Args:
            key(str): The key to decode

        Returns:
            (dict): A dictionary containing the components of the key
        """
        result = {}
        parts = key.split('&')
        result["collection"] = int(parts[1])
        result["experiment"] = int(parts[2])
        result["channel"] = int(parts[3])
        result["resolution"] = int(parts[4])
        result["x_index"] = int(parts[5])
        result["y_index"] = int(parts[6])
        result["z_index"] = int(parts[7])
        result["t_index"] = int(parts[8])

        return result

    def decode_chunk_key(self, key):
        """A method to decode the chunk key

        Args:
            key(str): The key to decode

        Returns:
            (dict): A dictionary containing the components of the key
        """
        result = {}
        parts = key.split('&')
        result["num_tiles"] = int(parts[1])
        result["collection"] = int(parts[2])
        result["experiment"] = int(parts[3])
        result["channel"] = int(parts[4])
        result["resolution"] = int(parts[5])
        result["x_index"] = int(parts[6])
        result["y_index"] = int(parts[7])
        result["z_index"] = int(parts[8])
        result["t_index"] = int(parts[9])

        return result
