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
from intern.remote.boss import BossRemote
from intern.resource.boss.resource import ChannelResource
import numpy as np
import time

from .path import PathProcessor
from .tile import TileProcessor


class InternPathProcessor(PathProcessor):
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
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        return ""


class InternTileProcessor(TileProcessor):
    """A Tile processor for a single image file identified by z index"""

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.remote = None
        self.channel = None

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
        self.remote = BossRemote()
        self.channel = ChannelResource(self.parameters["channel"],
                                       self.parameters["collection"],
                                       self.parameters["experiment"])
        self.channel = self.remote.get_project(self.channel)

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
        # Compute cutout args
        x_rng = [self.parameters["x_tile"] * x_index + self.parameters["x_offset"],
                 self.parameters["x_tile"] * (x_index + 1) + self.parameters["x_offset"]]
        y_rng = [self.parameters["y_tile"] * y_index + self.parameters["y_offset"],
                 self.parameters["y_tile"] * (y_index + 1) + self.parameters["y_offset"]]
        z_rng = [z_index + self.parameters["z_offset"], z_index + 1 + self.parameters["z_offset"]]

        if z_index + self.parameters["z_offset"] < 0:
            data = np.zeros((self.parameters["x_tile"], self.parameters["y_tile"]),
                            dtype=np.int32, order="C")
        else:
            # Get data
            cnt = 0
            while cnt < 5:
                try:
                    data = self.remote.get_cutout(self.channel, self.parameters["resolution"], x_rng, y_rng, z_rng)
                    data = np.asarray(data, np.uint32)
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
