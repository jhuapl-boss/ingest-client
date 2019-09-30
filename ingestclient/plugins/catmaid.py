# Copyright 2019 The Johns Hopkins University Applied Physics Laboratory
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
from io import BytesIO
import time

import requests as req
from intern.remote.boss import BossRemote
from intern.resource.boss.resource import ChannelResource
from intern.service.boss.v1.volume import CacheMode

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
            raise IndexError("Invalid Tile Z-Index: {} Z-Extent: {}".format(z_index, self.parameters["ingest_job"]["extent"]["z"][1]))

        if x_index > self.parameters["ingest_job"]["extent"]["x"][1] / self.parameters["ingest_job"]["tile_size"]["x"] - 1:
            raise IndexError("Invalid Tile X-Index: {} X-Extent: {} X-TileSize: {}".format(x_index,
                                                                                           self.parameters["ingest_job"]["extent"]["x"][1],
                                                                                           self.parameters["ingest_job"]["tile_size"]["x"]))

        if y_index > self.parameters["ingest_job"]["extent"]["y"][1] / self.parameters["ingest_job"]["tile_size"]["y"] - 1:
            raise IndexError("Invalid Tile Y-Index: {} Y-Extent: {} Y-TileSize: {}".format(y_index,
                                                                                           self.parameters["ingest_job"]["extent"]["y"][1],
                                                                                           self.parameters["ingest_job"]["tile_size"]["y"]))

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

class CatmaidURLPathProcessor(PathProcessor):
    """Class for simple image stacks that only increment in Z, uses the dynamic filesystem utility"""
    def __init__(self):
        """Constructor to add custom class var"""
        PathProcessor.__init__(self)

    def setup(self, parameters):
        """Set the params


        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        pass

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile

        Args:
            Ignored
        Returns:
            (str): Empty string

        """
        return ""


class CatmainURLTileProcessor(TileProcessor):
    """A Tile processor for a single image file identified by z index"""

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)

    def setup(self, parameters):
        """ Method to load the file for uploading data. Assumes intern token is set via environment variable or config
        default file

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "x_offset": offset to apply when querying the Boss
                                         "y_offset": offset to apply when querying the Boss
                                         "z_offset": offset to apply when querying the Boss
                                         "x_tile": size of a tile in x dimension
                                         "y_tile": size of a tile in y dimension
                                         "collection": source collection
                                         "experiment": source experiment
                                         "channel": source channel
                                         "resolution": source resolution

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, file_path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the image file.

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        CATMAID_URL = self.parameters["url"]
        FORMAT = self.parameters["filetype"]

        if z_index + self.parameters["z_offset"] < 0:
            data = np.zeros((self.parameters["x_tile"], self.parameters["y_tile"]),
                            dtype=np.int32, order="C")
        else:
            # Get data
            cnt = 0
            while cnt < 5:
                try:
                    url = CATMAID_URL + str(z_index) + '/' + str(y_index) + '/' + str(x_index) + "." + FORMAT
                    r = req.get(url)
                    if r.status_code == 403:
                        print("=== \nRequest Err:{} \n{} \nreplacing with Zeros".format(r.status_code, url))
                        data = np.zeros((self.parameters["x_tile"], self.parameters["y_tile"]), dtype=np.int32, order="C")
                    elif r.status_code == 200:
                        data = Image.open(BytesIO(r.content))
                        data = np.asarray(data, np.uint32)
                    else: 
                        print("=== \nRequest Err:{}; attempting again".format(r.status_code))
                        print(url)
                        raise Exception
                    break
                except Exception as err:
                    if cnt > 5:
                        raise err
                    cnt += 1
                    time.sleep(10)

        # Save sub-img to png and return handle
        upload_img = Image.fromarray(np.squeeze(data))
        output = six.BytesIO()
        upload_img.save(output, format="TIFF")

        # Send handle back
        return output
