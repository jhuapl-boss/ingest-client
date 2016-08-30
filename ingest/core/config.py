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
import json
from abc import ABCMeta

import six
from ingest.core.validator import Validator


@six.add_metaclass(ABCMeta)
class Configuration(object):
    def __init__(self, config_file=None):
        """
        A class to implement the object store for cuboid storage

        Args:
            config_file(str): Absolute path to an ingest configuration file
        """
        self.config_file = config_file
        self.config_data = None
        self.validator = None
        self.schema = None
        self.backend = None

        # Properties of ingest after creation
        self.credentials = None
        self.ingest_job_id = None
        self.upload_job_queue = None

        # If a configuration file was provided, load it now
        if config_file:
            self.load(config_file)

    def load(self, config_file):
        """
        Method to load the configuration file, the configuration schema, and select the correct validator and backend

        Args:
            config_file(str): Absolute path to an ingest configuration file

        Returns:
            None

        """
        with open(config_file, 'r') as file_handle:
            self.config_data = json.load(file_handle)

        # Setup Validator while sanitizing input
        if any(x in self.config_data["schema"]["validator"] for x in [";", ".", "import"]):
            raise ValueError("Schema Validator Class contains dangerous syntax. Please only list the Class Name.")
        else:
            self.validator = Validator.factory(self.config_data["schema"]["validator"], config_file)

        # Setup Backend  while sanitizing input

    def to_json(self):
        """
        Method to return a JSON string containing the configuration

        Returns:
            (str): JSON encoded config data
        """
        return json.dumps(self.config_data)

    def validate(self):
        """

        Args:


        Returns:
            (dict): Dictionary of "info", "warning", "errors"

        """
        return self.validator(self.config_data)

    def create(self):
        """
        Method to create the ingest job by calling the backend.

        Sets self.credentials, self.ingest_job_id, and self.upload_job_queue

        Args:

        Returns:
            None
        """
        try:
            self.credentials, self.ingest_job_id, self.upload_job_queue = self.backend.create(self.config_data)
        except Exception as e:
            raise Exception("Failed to create ingest job using the backend service: {}".format(e))

    def resume(self, ingest_job_id):
        """
        Method to resume an ingest job by calling the backend to get

        Sets self.credentials, self.ingest_job_id, and self.upload_job_queue

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            None
        """
        try:
            self.ingest_job_id = ingest_job_id
            self.credentials, self.upload_job_queue = self.backend.resume(ingest_job_id)
        except Exception as e:
            raise Exception("Failed to create ingest job using the backend service: {}".format(e))

