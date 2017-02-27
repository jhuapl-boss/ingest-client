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
from math import floor
import os
import re

from ..utils.filesystem import DynamicFilesystemAbsPath
from .path import PathProcessor
from .tile import TileProcessor


def load_tiff_multipage(tiff_filename, dtype='uint16'):
    """
    Load a multipage tiff into a single variable in x,y,z format.

    Arguments:
        tiff_filename:     Filename of source data
        dtype:             data type to use for the returned tensor

    Returns:
        Array containing contents from input tiff file in tyx order
    """
    if not os.path.isfile(tiff_filename):
        raise IOError('File not found: {}'.format(tiff_filename))

    # load the data from multi-layer TIF files
    data = Image.open(tiff_filename)

    im = []
    while True:
        # Get all slices from file
        img_slice = np.array(data, dtype=dtype)
        if img_slice.ndim == 2:
            img_slice = img_slice[np.newaxis, ...]
        im.append(img_slice)

        try:
            data.seek(data.tell()+1)
        except EOFError:
            break  # end of file

    im = np.concatenate(im, axis=0)
    return im


class SingleTimeTiffPathProcessor(PathProcessor):
    def setup(self, parameters):
        """Set the params - for this just store where the file is located

        MUST HAVE THE CUSTOM PARAMETERS: "z_<index>": "<filename>"

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
        if t_index < self.parameters["ingest_job"]["extent"]["t"][0] or t_index >= self.parameters["ingest_job"]["extent"]["t"][1]:
            raise IndexError("Invalid Tile T-Index: {}".format(t_index))

        if z_index < self.parameters["ingest_job"]["extent"]["z"][0] or z_index >= self.parameters["ingest_job"]["extent"]["z"][1]:
            raise IndexError("Invalid Tile Z-Index: {}".format(z_index))

        if x_index > self.parameters["ingest_job"]["extent"]["x"][1] / self.parameters["ingest_job"]["tile_size"]["x"] - 1:
            raise IndexError("Invalid Tile X-Index: {}".format(x_index))

        if y_index > self.parameters["ingest_job"]["extent"]["y"][1] / self.parameters["ingest_job"]["tile_size"]["y"] - 1:
            raise IndexError("Invalid Tile Y-Index: {}".format(y_index))

        return self.parameters['z_{}'.format(z_index)]


class SingleTimeTiffTileProcessor(TileProcessor):
    """A Tile processor for a file where a multi-page TIFF contains all time points for a single z-slice"""
    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.data = None

    def setup(self, parameters):
        """ Method to load the file for uploading - a very naive approach

        MUST HAVE THE CUSTOM PARAMETER: "datatype": "<uint8|uint16>"

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters
        self.data = {}

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
        # Load the file into memory
        if "z_{}".format(z_index) not in self.data:
            # storing slices in tyx
            self.data["z_{}".format(z_index)] = load_tiff_multipage(file_path, dtype=self.parameters["datatype"])

        im = self.data["z_{}".format(z_index)][t_index, :, :]

        # Compute matrix indices
        x_start = self.parameters["ingest_job"]["tile_size"]["x"] * x_index
        x_stop = self.parameters["ingest_job"]["tile_size"]["x"] * (x_index + 1)
        y_start = self.parameters["ingest_job"]["tile_size"]["y"] * y_index
        y_stop = self.parameters["ingest_job"]["tile_size"]["y"] * (y_index + 1)

        # TODO: Verify handles will be closed properly and memory reclaimed
        # Save img to png and return handle
        tile_data = Image.fromarray(im[y_start:y_stop, x_start:x_stop], 'I;16')

        output = six.BytesIO()
        tile_data.save(output, format="TIFF")

        # Send handle back
        return output


class TiffMultiFileHyperStackPathProcessor(PathProcessor):
    """A Path processor for a hyperstack stored across multiple multi-page TIFF files, with the time dimension split
    across files"""

    def __init__(self):
        """Constructor to add custom class var"""
        PathProcessor.__init__(self)
        self.regex = None

    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "extension": "tiff|tif",
                                         "base_filename": the base filename, see below for how this is parsed,
                                         "time_chunk_size": <int>  # The number of time samples in a single file

        base_filename string identifies how to insert the z-index value into the filename. Identify a place to insert
        the z_index with "<>".  If you want to offset add o:number. If you want to zero pad add p:number"

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
        Method to compute the file path for the indicated time sample

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        # Create base filename
        matches = self.regex.findall(self.parameters['base_filename'])

        # Compute file number
        file_number = int(floor(t_index / int(self.parameters["time_chunk_size"])))

        base_str = self.parameters['base_filename']
        for m in matches:
            if m[0]:
                # there is an offset
                t_val = int(m[0].split(':')[1]) + file_number
            else:
                t_val = file_number

            if m[1]:
                # There is zero padding
                t_str = str(t_val).zfill(int(m[1].split(':')[1]))
            else:
                t_str = str(t_val)

            base_str = base_str.replace("<{}{}>".format(m[0], m[1]), t_str)

        # prepend root, append extension
        return os.path.join(self.parameters['root_dir'], "{}.{}".format(base_str, self.parameters['extension']))


class TiffMultiFileHyperStackTileProcessor(TileProcessor):
    """A Tile processor for multi-channel, multi-slice, time-series datasets stored as a hyperstack in a multi-page tiff
    """

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.fs = None

    def setup(self, parameters):
        """ Method to load the file for uploading

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "time_chunk_size": <int>,
                                         "num_z_slices": <int>,
                                         "num_channels": <int>,
                                         "channel_index": <int>,
                                         "filesystem": "<s3|local>",
                                         "bucket": (if s3 filesystem)

        Returns:
            None
        """
        self.parameters = parameters
        self.fs = DynamicFilesystemAbsPath(parameters['filesystem'], parameters)

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
        file_path = self.fs.get_file(file_path)

        # Open Tiff Hyper-Stack
        tiff_file = Image.open(file_path)

        # Compute frame Number
        frame_num = ((self.parameters["num_z_slices"] * self.parameters["num_channels"]) * t_index) + \
                    (z_index * self.parameters["num_channels"]) + (self.parameters["channel_index"])

        tiff_file.seek(frame_num % self.parameters["time_chunk_size"])

        tile_data = np.array(tiff_file, dtype=np.uint16)
        upload_img = Image.fromarray(tile_data, 'I;16')

        output = six.BytesIO()
        upload_img.save(output, format="TIFF")

        # Send handle back
        return output
