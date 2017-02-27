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
from pkg_resources import resource_filename
import os


@six.add_metaclass(ABCMeta)
class PathProcessor(object):
    def __init__(self):
        """
        A class to implement a path processor, which converts from parameters and tile indices
        to an absolute file path

        Args:
        """
        self.parameters = None

    @abstractmethod
    def setup(self, parameters):
        """
        Method to initialize the path processor based on custom parameters from the configuration file

        e.g. Connect to a database, etc.

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        return NotImplemented


class TestPathProcessor(PathProcessor):
    """Example processor for unit tests"""
    def setup(self, parameters):
        """
        Method to initialize the path processor based on custom parameters from the configuration file

        e.g. Connect to a database, etc.

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile

        Test processor always returns path to the same dummy file!

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        return os.path.join(resource_filename("ingestclient", "test/data"), "test_tile.png")


class TestPassThroughPathProcessor(PathProcessor):
    """Example processor for scale testing"""
    def setup(self, parameters):
        """

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile

        Test processor always returns empty string since random data is to be generated during scale testing.

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        return ""
