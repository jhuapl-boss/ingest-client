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
import numpy as np

XYZ_ORDER = 0
ZYX_ORDER = 1
XYZT_ORDER = 2
TZYX_ORDER = 3

@six.add_metaclass(ABCMeta)
class ChunkProcessor(object):
    def __init__(self):
        """
        A class that implements a chunk processor which outputs ndarrays for uploading

        Args:
        """
        self.parameters = None

    @abstractmethod
    def setup(self, parameters):
        """
        Method to initialize the chunk processor based on custom parameters from the configuration file

        e.g. Connect to a database, etc.

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def process(self, file_path, x_index, y_index, z_index):
        """
        Method to take a chunk indices and return an ndarray with the correct data

        Args:
            file_path(str): An absolute file path for the specified chunk
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension

        Returns:
            (np.ndarray, int): ndarray for the specified chunk, array order (XYZ_ORDER, TZYX_ORDER, etc)
        """
        return NotImplemented
