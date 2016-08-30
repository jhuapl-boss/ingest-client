# Copyright 2016 NeuroData (http://neurodata.io)
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


@six.add_metaclass(ABCMeta)
class Backend(object):
    def __init__(self, config):
        """
        A class to implement a backend that supports the ingest service

        Args:

        """
        self.ingest_job_id = None
        self.config = config
        self.setup()

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


class BossBackend(Backend):
    def __init__(self, config):
        """
        A class to implement a backend that supports the ingest service

        Args:

        """
        self.host = None
        self.config = config
        self.setup()

    def setup(self):
        """
        Method to configure the backend based on configuration parameters in the config file

        Args:

        Returns:
            None


        """
        self.host = "{}://{}".format(self.config["protocol"], self.config["host"])

    def create(self, config_dict):
        """
        Method to upload the config data to the backend to create an ingest job

        Args:
            config_dict(dict): config data

        Returns:
            (int): The returned ingest_job_id


        """
        r = requests.post('{}/ingest/job/'.format(self.host), json=config_dict)

        if r.status_code != 201:
            return "Failed to create ingest job. Verify configuration file."
        else:
            self.ingest_job_id = r.json()['id']

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
