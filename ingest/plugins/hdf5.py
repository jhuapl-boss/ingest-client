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
import re
import os
from ingest.utils.filesystem import DynamicFilesystemAbsPath
import h5py
import numpy as np

from ingest.plugins.path import PathProcessor
from ingest.plugins.tile import TileProcessor


class Hdf5TimeSeriesPathProcessor(PathProcessor):
    """A Path processor for time-series, multi-channel data (e.g. calcium imaging)

    Assumes the data is stored (t,y,z, channel) in individual hdf5 files, with 1 hdf5 file per z-slice

    """
    def __init__(self):
        """Constructor to add custom class var"""
        PathProcessor.__init__(self)
        self.regex = None

    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "extension": "hdf5|h5",
                                         "base_filename": the base filename, see below for how this is parsed,

        base_filename string identifies how to insert the z-index value into the filename. Identify a place to insert
        the z_index with "<>".  If you want to offset add o:number. If you want to zero pad add z:number"

        my_base_<> -> my_base_0, my_base_1, my_base_2
        <o:200>_my_base_<p:4> -> 200_my_base_0000, 201_my_base_0001, 202_my_base_0002

        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters
        self.regex = re.compile('<(o:\d+)?(p:\d+)?>')

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated Z-slice

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        if z_index >= self.parameters["ingest_job"]["extent"]["z"][1]:
            raise IndexError("Z-index out of range")

        # Create base filename
        matches = self.regex.findall(self.parameters['base_filename'])

        base_str = self.parameters['base_filename']
        for m in matches:
            if m[0]:
                # there is an offset
                z_val = int(m[0].split(':')[1]) + z_index
            else:
                z_val = z_index

            if m[1]:
                # There is zero padding
                z_str = str(z_val).zfill(int(m[1].split(':')[1]))
            else:
                z_str = str(z_val)

            base_str = base_str.replace("<{}{}>".format(m[0], m[1]), z_str)

        # prepend root, append extension
        return os.path.join(self.parameters['root_dir'], "{}.{}".format(base_str, self.parameters['extension']))


class Hdf5TimeSeriesTileProcessor(TileProcessor):
    """A Tile processor for time-series, multi-channel data (e.g. calcium imaging)

    Assumes the data is stored (t,y,x, channel) in individual hdf5 files, with 1 hdf5 file per z-slice

    """

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.fs = None

    def setup(self, parameters):
        """ Method to load the file for uploading

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "upload_format": "<png|tif>",
                                         "channel_index": integer,
                                         "scale_factor": float,
                                         "dataset": str,
                                         "filesystem": "<s3|local>",
                                         "bucket": (if s3 filesystem)

        Returns:
            None
        """
        self.parameters = parameters
        self.fs = DynamicFilesystemAbsPath(parameters['filesystem'], parameters)

    def process(self, file_path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the image file. Can break the image into smaller tiles to help make ingest go smoother, but
        currently must be perfectly divisible

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        file_path = self.fs.get_file(file_path)

        x_range = [self.parameters["ingest_job"]["tile_size"]["x"] * x_index,
                   self.parameters["ingest_job"]["tile_size"]["x"] * (x_index + 1)]
        y_range = [self.parameters["ingest_job"]["tile_size"]["y"] * y_index,
                   self.parameters["ingest_job"]["tile_size"]["y"] * (y_index + 1)]

        # Open hdf5
        h5_file = h5py.File(file_path, 'r')

        # Save sub-img to png and return handle
        tile_data = np.array(h5_file[self.parameters['dataset']][t_index,
                                                                 y_range[0]:y_range[1],
                                                                 x_range[0]:x_range[1],
                                                                 int(self.parameters['channel_index'])])

        tile_data = np.multiply(tile_data, self.parameters['scale_factor'])
        tile_data = tile_data.astype(np.uint16)
        upload_img = Image.fromarray(tile_data, 'I;16')

        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output


class Hdf5TimeSeriesLabelTileProcessor(TileProcessor):
    """A Tile processor for label data packed in a time-series, multi-channel HDF5 (e.g. ROIs for calcium imaging)

    Assumes the data is stored (y,x) in individual hdf5 files, with 1 hdf5 file per z-slice

    """

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.fs = None

    def setup(self, parameters):
        """ Method to load the file for uploading

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "upload_format": "<png|tif>",
                                         "dataset": str,
                                         "filesystem": "<s3|local>",
                                         "bucket": (if s3 filesystem)

        Returns:
            None
        """
        self.parameters = parameters
        self.fs = DynamicFilesystemAbsPath(parameters['filesystem'], parameters)

    def process(self, file_path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the image file. Can break the image into smaller tiles to help make ingest go smoother, but
        currently must be perfectly divisible

        Args:
            file_path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        file_path = self.fs.get_file(file_path)

        x_range = [self.parameters["ingest_job"]["tile_size"]["x"] * x_index,
                   self.parameters["ingest_job"]["tile_size"]["x"] * (x_index + 1)]
        y_range = [self.parameters["ingest_job"]["tile_size"]["y"] * y_index,
                   self.parameters["ingest_job"]["tile_size"]["y"] * (y_index + 1)]

        # Open hdf5
        h5_file = h5py.File(file_path, 'r')

        # Save sub-img to png and return handle
        tile_data = np.array(h5_file[self.parameters['dataset']][y_range[0]:y_range[1], x_range[0]:x_range[1]])
        tile_data = tile_data.astype(np.uint32)
        upload_img = Image.fromarray(tile_data, 'I')

        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output