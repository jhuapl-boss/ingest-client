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
from __future__ import absolute_import
import six
from PIL import Image
import numpy as np
import os

from .path import PathProcessor
from .tile import TileProcessor


class CatmaidFileImageStackZoomLevelPathProcessor(PathProcessor):
    """Class for Catmaid File-based image stacks with zoom levels, Tile source type 4 in the documentation"""
    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "filetype": "<png|tif|jpg>"
        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile - For this, it's always the same file for each Z slice

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        if t_index != 0:
            raise IndexError("CATMAID File Image Stack format does not support non-zero time index")

        if z_index < self.parameters["ingest_job"]["extent"]["z"][0] or z_index >= self.parameters["ingest_job"]["extent"]["z"][1]:
            raise IndexError("Invalid Tile Z-Index: {}".format(z_index))

        if x_index > self.parameters["ingest_job"]["extent"]["x"][1] / self.parameters["ingest_job"]["tile_size"]["x"] - 1:
            raise IndexError("Invalid Tile X-Index: {}".format(x_index))

        if y_index > self.parameters["ingest_job"]["extent"]["y"][1] / self.parameters["ingest_job"]["tile_size"]["y"] - 1:
            raise IndexError("Invalid Tile Y-Index: {}".format(y_index))

        filename = "{}_{}.{}".format(y_index, x_index, self.parameters["filetype"])
        return os.path.join(self.parameters["root_dir"], "{}".format(self.parameters["ingest_job"]["resolution"]), "{}".format(z_index), filename)


class CatmaidFileImageStackZoomLevelTileProcessor(TileProcessor):
    """A Tile processor for a file where a multi-page TIFF contains all time points for a single z-slice"""
    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.data = None

    def setup(self, parameters):
        """ Method to load the file for uploading - a very naive approach

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, file_path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the configuration file and select the correct validator and backend

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        # Save img to png and return handle
        tile_data = Image.open(file_path)

        output = six.BytesIO()
        tile_data.save(output, format=self.parameters["filetype"].upper())

        # Send handle back
        return output

class CatmaidDirectoryImageStackPathProcessor(PathProcessor):
    """Class for Catmaid Directory-based image stacks, Tile source type 5 in the documentation"""
    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "filetype": "<png|tif|jpg>"
        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile - For this, it's always the same file for each Z slice

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        if t_index != 0:
            raise IndexError("CATMAID File Image Stack format does not support non-zero time index")

        if z_index < self.parameters["ingest_job"]["extent"]["z"][0] or z_index >= self.parameters["ingest_job"]["extent"]["z"][1]:
            raise IndexError("Invalid Tile Z-Index: {}".format(z_index))

        if x_index > self.parameters["ingest_job"]["extent"]["x"][1] / self.parameters["ingest_job"]["tile_size"]["x"] - 1:
            raise IndexError("Invalid Tile X-Index: {}".format(x_index))

        if y_index > self.parameters["ingest_job"]["extent"]["y"][1] / self.parameters["ingest_job"]["tile_size"]["y"] - 1:
            raise IndexError("Invalid Tile Y-Index: {}".format(y_index))

        filename = "{}.{}".format(y_index, x_index, self.parameters["filetype"])
        return os.path.join(self.parameters["root_dir"], "{}".format(self.parameters["ingest_job"]["resolution"]), "{}".format(z_index), "{}".format(y_index), filename)


class CatmaidDirectoryImageStackTileProcessor(TileProcessor):
    """A Tile processor for a file where a multi-page TIFF contains all time points for a single z-slice"""
    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.data = None

    def setup(self, parameters):
        """ Method to load the file for uploading - a very naive approach

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, file_path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the configuration file and select the correct validator and backend

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        # Save img to png and return handle
        tile_data = Image.open(file_path)

        output = six.BytesIO()
        tile_data.save(output, format=self.parameters["filetype"].upper())

        # Send handle back
        return output

class CatmaidFileImageStackPathProcessor(PathProcessor):
    """Class for Catmaid File-based image stacks, Tile source type 1 in the documentation"""
    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "filetype": "<png|tif|jpg>"
        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile - For this, it's always the same file for each Z slice

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        if t_index != 0:
            raise IndexError("CATMAID File Image Stack format does not support non-zero time index")

        if z_index < self.parameters["ingest_job"]["extent"]["z"][0] or z_index >= self.parameters["ingest_job"]["extent"]["z"][1]:
            raise IndexError("Invalid Tile Z-Index: {}".format(z_index))

        if x_index > self.parameters["ingest_job"]["extent"]["x"][1] / self.parameters["ingest_job"]["tile_size"]["x"] - 1:
            raise IndexError("Invalid Tile X-Index: {}".format(x_index))

        if y_index > self.parameters["ingest_job"]["extent"]["y"][1] / self.parameters["ingest_job"]["tile_size"]["y"] - 1:
            raise IndexError("Invalid Tile Y-Index: {}".format(y_index))

        filename = "{}_{}_{}.{}".format(y_index, x_index, self.parameters["ingest_job"]["resolution"], self.parameters["filetype"])
        return os.path.join(self.parameters["root_dir"], "{}".format(z_index), filename)


class CatmaidFileImageStackTileProcessor(TileProcessor):
    """A Tile processor for a file where a multi-page TIFF contains all time points for a single z-slice"""
    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.data = None

    def setup(self, parameters):
        """ Method to load the file for uploading - a very naive approach

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, file_path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the configuration file and select the correct validator and backend

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        # Save img to png and return handle
        tile_data = Image.open(file_path)

        output = six.BytesIO()
        tile_data.save(output, format=self.parameters["filetype"].upper())

        # Send handle back
        return output
