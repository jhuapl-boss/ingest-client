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


class Backend(metaclass=ABCMeta):
    def __init__(self):
        """
        A class to implement a backend that supports the ingest service

        Args:

        """

    @abstractmethod
    def create(self, data):
        """
        Method to upload the config data to the backend to create an ingest job

        Args:
            data(dict): A dictionary of configuration parameters

        Returns:
            (dict, int, str): The returned credentials, ingest_job_id, and SQS upload_job_queue


        """
        return NotImplemented

    @abstractmethod
    def resume(self, ingest_job_id):
        """
        Method to upload the config data to the backend to create an ingest job

        Args:
            ingest_job_id(int): The ID of the job you'd like to resume processing

        Returns:
            (dict, str): The returned credentials and SQS upload_job_queue for the provided ingest job id


        """
        return NotImplemented
