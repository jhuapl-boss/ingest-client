# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
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
import hashlib
from six.moves import configparser
import os


@six.add_metaclass(ABCMeta)
class Backend(object):
    def __init__(self, config):
        """
        A class to implement a backend that supports the ingest service

        Args:
            config (dict): Dictionary of parameters from the "backend" section of the config file

        """
        self.config = config
        self.sqs = None
        self.queue = None

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

        Job Status: {0: Preparing, 1: Uploading, 2: Complete}

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (int, dict, str): The job status, AWS credentials, and SQS upload_job_queue for the provided ingest job id


        """
        return NotImplemented

    @abstractmethod
    def cancel(self, ingest_job_id):
        """
        Method to cancel an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            None


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

    def setup_upload_queue(self, credentials, upload_queue, region="us-east-1"):
        """
        Method to create a connection to the upload task queue

        Args:
            credentials(dict): AWS credentials
            upload_queue(str): The URL for the upload SQS queue
            region(str): The AWS region where the SQS queue exists

        Returns:
            None

        """
        self.sqs = boto3.resource('sqs', region_name=region, aws_access_key_id=credentials["id"],
                                  aws_secret_access_key=credentials["secret"])
        self.queue = self.sqs.Queue(url=upload_queue)

    # TODO: Possibly remove if ndingest lib is used as a dependency
    @abstractmethod
    def encode_object_key(self, project_info, resolution, x_index, y_index, z_index, t_index=0):
        """

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
        self.api_version = "v0.5"

    def setup(self, api_token=None):
        """
        Method to configure the backend based on configuration parameters in the config file

        Args:

        Returns:
            None


        """
        self.host = "{}://{}".format(self.config["client"]["backend"]["protocol"],
                                     self.config["client"]["backend"]["host"])

        # Load API creds from ndio if needed.
        if not api_token:
            try:
                cfg_parser = configparser.ConfigParser()
                cfg_parser.read(os.path.expanduser("~/.ndio/ndio.cfg"))
                api_token = cfg_parser.get("Project Service", "token")
            except KeyError as e:
                print("API Token not provided and ndio is not configured (config file located at ~/.ndio/ndio.cfg. Failed to setup backend: {}".format(e))

        self.api_headers = {'Authorization': 'Token ' + api_token, 'Accept': 'application/json'}

    def create(self, config_dict):
        """
        Method to upload the config data to the backend to create an ingest job

        Args:
            config_dict(dict): config data

        Returns:
            (int): The returned ingest_job_id


        """
        r = requests.post('{}/{}/ingest/job/'.format(self.host, self.api_version), json=config_dict,
                          headers=self.api_headers)

        if r.status_code != 201:
            return "Failed to create ingest job. Verify configuration file."
        else:
            return r.json()['ingest_job_id']

    def join(self, ingest_job_id):
        """
        Method to join an ingest job upload

        Job Status: {0: Preparing, 1: Uploading, 2: Complete}

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (int, dict, str, str): The job status, AWS credentials,
                                    and SQS upload_job_queue for the provided ingest job id, and the tile bucket name


        """
        r = requests.get('{}/{}/ingest/job/{}'.format(self.host, self.api_version, ingest_job_id),
                          headers=self.api_headers)

        if r.status_code != 200:
            raise Exception("Failed to join ingest job.")
        else:
            result = r.json()
            if result['ingest_job_status'] < 2:
                self.setup_upload_queue(result['credentials'], result['upload_queue'], region="us-east-1")
            return result['ingest_job_status'], result['credentials'], result['upload_queue'], result['tile_bucket']

    def cancel(self, ingest_job_id):
        """
        Method to cancel an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            None


        """
        r = requests.delete('{}/{}/ingest/job/{}'.format(self.host, self.api_version, ingest_job_id),
                            headers=self.api_headers)

        if r.status_code != 200:
            raise Exception("Failed to join ingest job.")

    def get_task(self):
        """
        Method to get an upload task

        Args:

        Returns:
            (str, str, dict): message_id, receipt_handle, message contents
        """
        # TODO: Possibly remove if ndingest lib is used as a dependency
        msg = self.queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=5)
        if msg:
            return msg[0].message_id, msg[0].receipt_handle, json.loads(msg[0].body)
        else:
            return None, None, None

    def encode_object_key(self, project_info, resolution, x_index, y_index, z_index, t_index=0):
        """

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
