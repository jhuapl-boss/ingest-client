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

from abc import ABCMeta, abstractmethod


class Configuration(metaclass=ABCMeta):
    def __init__(self, file_path):
        """
        A class to implement the object store for cuboid storage

        Args:
            file_path(str): Absolute path to an ingest configuration file
        """
        self.config_file = file_path
        self.config_data = self.load(file_path)
        self.validator = None
        self.backend = None

        # Properties of ingest after creation
        self.credentials = None
        self.ingest_job_id = None
        self.upload_job_queue = None

    @abstractmethod
    def load(self, file_path):
        """
        Method to load the configuration file and select the correct validator and backend

        Args:
            file_path(str): Absolute path to an ingest configuration file

        Returns:
            None

        """
        return NotImplemented

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
            return True
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
            return True
        except Exception as e:
            raise Exception("Failed to create ingest job using the backend service: {}".format(e))

