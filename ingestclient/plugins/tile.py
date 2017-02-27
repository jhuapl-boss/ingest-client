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
from PIL import Image


@six.add_metaclass(ABCMeta)
class TileProcessor(object):
    def __init__(self):
        """
        A class to implement a tile processor which outputs a list of file handles for uploading

        Args:
        """
        self.parameters = None

    @abstractmethod
    def setup(self, parameters):
        """
        Method to initialize the tile processor based on custom parameters from the configuration file

        e.g. Open a multi-page tiff, connect to a database, etc.

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def process(self, file_path, x_index, y_index, z_index, t_index=None):
        """
        Method to take a file path and tile indices and return a file handle to the correct data

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        return NotImplemented


class TestTileProcessor(TileProcessor):
    """Example processor for unit tests"""

    def setup(self, parameters):
        """
        Method to initialize the tile processor based on custom parameters from the configuration file

        e.g. Open a multi-page tiff, connect to a database, etc.

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, file_path, x_index, y_index, z_index, t_index=None):
        """
        Method to take a file path and tile indices and return a file handle to the correct data

        Always just returns the handle to the file

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        return open(file_path, 'rb')


class TestRandomTileProcessor(TileProcessor):
    """Example processor for scale tests"""

    def setup(self, parameters):
        """
        Method to initialize the tile processor based on custom parameters from the configuration file

        e.g. Open a multi-page tiff, connect to a database, etc.

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, file_path, x_index, y_index, z_index, t_index=None):
        """
        Generate a random tile

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        tile = np.random.randint(1, 254, size=(self.parameters["ingest_job"]["tile_size"]["y"],
                                               self.parameters["ingest_job"]["tile_size"]["x"]), dtype=np.uint8)
        tile_data = Image.fromarray(tile)
        output = six.BytesIO()
        tile_data.save(output, format="TIFF")

        return output
