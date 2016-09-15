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
        Array containing contents from input tiff file in xyz order
    """
    if not os.path.isfile(tiff_filename):
        raise RuntimeError('could not find file "%s"' % tiff_filename)

    # load the data from multi-layer TIF files
    data = Image.open(tiff_filename)

    im = []

    while True:

        Xi = np.array(data, dtype=dtype)
        if Xi.ndim == 2:
            Xi = Xi[np.newaxis, ...]  # add slice dimension
        im.append(Xi)

        try:
            data.seek(data.tell()+1)
        except EOFError:
            break  # this just means hit end of file (not really an error)

    im = np.concatenate(im, axis=0)  # list of 2d -> tensor
    im = np.rollaxis(im, 1)
    im = np.rollaxis(im, 2)

    return im


class SingleTimeTiffPathProcessor(PathProcessor):
    def setup(self, parameters):
        """Set the params - for this just store where the file is located

        MUST HAVE THE CUSTOM PARAMETERS "z_{index}": "{filename}"

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile - For this, it's always the same file

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension
            t_index(int): The time index

        Returns:
            (str): An absolute file path that contains the specified data

        """
        return self.parameters['z_{}'.format(z_index)]


class SingleTimeTiffTileProcessor(TileProcessor):
    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.data = None

    def setup(self, parameters):
        """ Method to load the file for uploading - a very naive approach!

        MUST HAVE THE CUSTOM PARAMETER "filename" indicating the file to upload

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        pass

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
        if not self.data["z_{}".format(z_index)]:
            data = load_tiff_multipage(file_path)
            data = np.rollaxis(data, 1)
            data = np.rollaxis(data, 2)
            self.data["z_{}".format(z_index)] = np.expand_dims(data, axis=1)

        # TODO: Verify handles will be closed properly and memory reclaimed
        # Save img to png and return handle
        tile_data = Image.fromarray(self.data["z_{}".format(z_index)][t_index, :, :, :])
        output = six.StringIO.StringIO()
        tile_data.save(output, format="PNG")

        # Send handle back
        return output
