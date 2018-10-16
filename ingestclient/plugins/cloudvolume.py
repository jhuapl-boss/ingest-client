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

from .path import PathProcessor
from .chunk import ChunkProcessor, XYZT_ORDER

from cloudvolume import CloudVolume

class CloudVolumePathProcessor(PathProcessor):
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


class CloudVolumeChunkProcessor(ChunkProcessor):
    """Chunk processor that utilizes Seung Lab's CloudVolume"""

    def __init__(self):
        ChunkProcessor.__init__(self)
        self.vol = None
        self.ingest_job = None

    def setup(self, parameters):
        """ Method to load the file for uploading data. Assumes intern token is set via environment variable or config
        default file

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETER: "cloudpath": location of CloudVolume data

        All other parameters are optional parameters that may be passed to the CloudVolume constructor

        Returns:
            None
        """
        self.parameters = parameters

        # Remove 'ingest_job' key so rest of parameters can be passed to the
        # CloudVolume constructor.
        self.ingest_job = self.parameters.pop('ingest_job')
        self.vol = CloudVolume(**self.parameters)

    def process(self, file_path, x_index, y_index, z_index):
        """
        Method to take a chunk indices and return an ndarray with the correct data

        Args:
            file_path(str): An absolute file path for the specified chunk
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension

        Returns:
            (np.ndarray, int): ndarray for the specified chunk, XYZT_ORDER
        """
        x_size = self.ingest_job["chunk_size"]["x"]
        y_size = self.ingest_job["chunk_size"]["y"]
        z_size = self.ingest_job["chunk_size"]["z"]

        x_start = x_index * x_size;
        y_start = y_index * y_size;
        z_start = z_index * z_size;

        x_stop = x_start + x_size
        y_stop = y_start + y_size
        z_stop = z_start + z_size

        if x_stop > self.vol.bounds.maxpt[0]:
            x_stop = self.vol.bounds.maxpt[0]
        if y_stop > self.vol.bounds.maxpt[1]:
            y_stop = self.vol.bounds.maxpt[1]
        if z_stop > self.vol.bounds.maxpt[2]:
            z_stop = self.vol.bounds.maxpt[2]

        return self.vol[x_start:x_stop, y_start:y_stop, z_start:z_stop], XYZT_ORDER
