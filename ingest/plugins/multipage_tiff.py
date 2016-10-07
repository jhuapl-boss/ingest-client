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
