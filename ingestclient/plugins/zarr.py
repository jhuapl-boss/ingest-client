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
from .chunk import ChunkProcessor, ZYX_ORDER
import numpy as np
import zarr


class ZarrPathProcessor(PathProcessor):
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


class ZarrChunkProcessor(ChunkProcessor):
    """Chunk processor that utilizes Seung Lab's CloudVolume"""

    def __init__(self):
        ChunkProcessor.__init__(self)
        self.vol = None
        self.ingest_job = None

    def setup(self, parameters):
        """ Method to load the file for uploading data. Assumes intern token is set via environment variable or config
        default file

        Args:
            parameters (dict): Parameters for the dataset to be processed. Must
                include the following keys:
                "backend" : [S3, GCS] for Amazon S3 and Google Cloud Service, respectively.
                "bucket" : name of the S3 or GCS bucket containing zarr file. 
                "volume_name" name of the volume in the zarr file (e.g. "raw")

        Returns:
            None
        """
        self.parameters = parameters
        self.ingest_job = self.parameters.pop("ingest_job")
        self.cloud_path = self.parameters["cloud_path"]
        self.volume_name = self.parameters["volume_name"]
        self.bucket = self.cloud_path.split('//')[1]
        
        if self.cloud_path.startswith("s3://"):
            from s3fs import S3FileSystem
            Zg = zarr.group(store=S3FileSystem().get_mapper(self.bucket))
        elif self.cloud_path.startswith("gs://"):
            from gcsfs import GCSFileSystem
            Zg = zarr.group(store=GCSFileSystem().get_mapper(self.bucket))
        else:
            raise ValueError("Cloudpath parameter must either start with 's3://' or 'gs://'.")
        
        self.vol = Zg[self.volume_name]

    def process(self, file_path, x_index, y_index, z_index):
        """
        Method to take a chunk indices and return an ndarray with the correct data

        Args:
            file_path(str): Ignored.
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension

        Returns:
            (np.ndarray, int): ndarray for the specified chunk, ZYX_ORDER
        """
        x_size = self.ingest_job["chunk_size"]["x"]
        y_size = self.ingest_job["chunk_size"]["y"]
        z_size = self.ingest_job["chunk_size"]["z"]

        x_start = x_index * x_size
        y_start = y_index * y_size
        z_start = z_index * z_size

        x_stop = x_start + x_size
        y_stop = y_start + y_size
        z_stop = z_start + z_size

        data = self.vol[z_start:z_stop, y_start:y_stop, x_start:x_stop]
        return data, ZYX_ORDER
