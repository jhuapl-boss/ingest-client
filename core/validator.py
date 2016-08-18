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


class Validator(ABCMeta):
    def __init__(self):
        """
        A class to implement the ingest job configuration file validator

        Args:

        """

    @abstractmethod
    def validate(self, data):
        """
        Method to load the configuration file and select the correct validator and backend

        Args:
            data(dict): A dictionary of configuration parameters

        Returns:
            (dict): Dictionary of "info", "warning", "error" messages


        """
        return NotImplemented
