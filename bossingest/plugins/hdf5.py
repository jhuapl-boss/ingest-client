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
import h5py
import numpy as np
from math import floor
import botocore
import logging


from ..utils.filesystem import DynamicFilesystemAbsPath
from .path import PathProcessor
from .tile import TileProcessor


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

    Assumes the data is stored (t, x, y, channel) in individual hdf5 files, with 1 hdf5 file per z-slice
    where x is the column dim and y is the row dim

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
                                                                 x_range[0]:x_range[1],
                                                                 y_range[0]:y_range[1],
                                                                 int(self.parameters['channel_index'])])

        tile_data = np.swapaxes(tile_data, 0, 1)
        tile_data = np.multiply(tile_data, self.parameters['scale_factor'])
        tile_data = tile_data.astype(np.uint16)
        upload_img = Image.fromarray(tile_data, 'I;16')

        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output


class Hdf5TimeSeriesLabelTileProcessor(TileProcessor):
    """A Tile processor for label data packed in a time-series, multi-channel HDF5 (e.g. ROIs for calcium imaging)

    Assumes the data is stored (x, y) in individual hdf5 files, with 1 hdf5 file per z-slice
        where x is the column dim and y is the row dim

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
        tile_data = np.array(h5_file[self.parameters['dataset']][x_range[0]:x_range[1], y_range[0]:y_range[1]])
        tile_data = np.swapaxes(tile_data, 0, 1)
        tile_data = tile_data.astype(np.uint32)
        upload_img = Image.fromarray(tile_data, 'I')

        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output


class Hdf5SlicePathProcessor(PathProcessor):
    """A Path processor for large slices stored in hdf5 files.

    Assumes the data is stored in a dataset and an optional offset is stored in a dataset

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


class Hdf5SliceTileProcessor(TileProcessor):
    """A Tile processor for large slices stored in hdf5 files.

    Assumes the data is stored in a dataset and an optional offset is stored in a dataset

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
                                         "data_name": str,
                                         "offset_name": str,
                                         "extent_name": str,
                                         "offset_origin_x": int,
                                         "offset_origin_y": int,
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

        # Compute global range
        tile_x_range = [self.parameters["ingest_job"]["tile_size"]["x"] * x_index,
                        self.parameters["ingest_job"]["tile_size"]["x"] * (x_index + 1)]
        tile_y_range = [self.parameters["ingest_job"]["tile_size"]["y"] * y_index,
                        self.parameters["ingest_job"]["tile_size"]["y"] * (y_index + 1)]

        # Open hdf5
        h5_file = h5py.File(file_path, 'r')

        # Compute range in actual data, taking offsets into account
        x_offset = h5_file[self.parameters['offset_name']][1]
        y_offset = h5_file[self.parameters['offset_name']][0]

        x_img_extent = h5_file[self.parameters['extent_name']][1]
        y_img_extent = h5_file[self.parameters['extent_name']][0]

        x_frame_offset = x_offset + self.parameters['offset_origin_x']
        y_frame_offset = y_offset + self.parameters['offset_origin_x']

        x1 = max(tile_x_range[0], x_frame_offset)
        y1 = max(tile_y_range[0], y_frame_offset)
        x2 = min(tile_x_range[1], x_frame_offset + x_img_extent)
        y2 = min(tile_y_range[1], y_frame_offset + y_img_extent)

        if self.parameters['datatype'] == "uint8":
            datatype = np.uint8
        elif self.parameters['datatype']== "uint16":
            datatype = np.uint16
        else:
            raise Exception("Unsupported datatype: {}".format(self.parameters['datatype']))

        # Allocate Tile
        tile_data = np.zeros((self.parameters["ingest_job"]["tile_size"]["y"],
                             self.parameters["ingest_job"]["tile_size"]["x"]),
                             dtype=datatype, order='C')

        # Copy sub-img to tile, save, return
        img_y_index_start = max(0, y1 - y_frame_offset)
        img_y_index_stop = max(0, y2 - y_frame_offset)

        img_x_index_start = max(0, x1 - x_frame_offset)
        img_x_index_stop = max(0, x2 - x_frame_offset)

        tile_data[y1-tile_y_range[0]:y2-tile_y_range[0],
                  x1 - tile_x_range[0]:x2 - tile_x_range[0]] = np.array(h5_file[self.parameters['data_name']][
                                                                        img_y_index_start:img_y_index_stop,
                                                                        img_x_index_start:img_x_index_stop])

        tile_data = tile_data.astype(datatype)
        upload_img = Image.fromarray(tile_data)

        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output


class Hdf5ChunkPathProcessor(PathProcessor):
    """A Path processor for chunks stored in hdf5 files.

    Assumes the data is stored in a dataset and the filename contains the loction. supports an xyz offset

    """
    def __init__(self):
        """Constructor to add custom class var"""
        PathProcessor.__init__(self)
        self.regex = None

    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "extension": "hdf5|h5",
                                         "prefix": Prefix for the filename
                                         "x_offset": the offset from 0 in the x dim
                                         "y_offset": the offset from 0 in the y dim
                                         "z_offset": the offset from 0 in the z dim
                                         "x_chunk_size": the chunk extent in the x dimension
                                         "y_chunk_size": the chunk extent in the y dimension
                                         "z_chunk_size": the chunk extent in the z dimension
                                         "use_python_convention" <bool>: a flag indicating if ranges use python convention

        filename format: prefix_xstart-xstop_ystart-ystop_zstart-zstop.h5

        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

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
        xstart = (x_index * self.parameters['x_chunk_size']) + self.parameters['x_offset']
        xstop = ((x_index + 1) * self.parameters['x_chunk_size']) + self.parameters['x_offset']
        if not self.parameters['use_python_convention']:
            xstop -= 1

        ystart = (y_index * self.parameters['y_chunk_size']) + self.parameters['y_offset']
        ystop = ((y_index + 1) * self.parameters['y_chunk_size']) + self.parameters['y_offset']
        if not self.parameters['use_python_convention']:
            ystop -= 1

        zstart = floor((z_index + self.parameters['z_offset']) / self.parameters['z_chunk_size'])
        zstop = ((zstart + 1) * self.parameters['z_chunk_size']) + self.parameters['z_offset']
        if not self.parameters['use_python_convention']:
            zstop -= 1
        zstart += self.parameters['z_offset']

        # prepend root, append extension
        filename = "{}_{}-{}_{}-{}_{}-{}.{}".format(self.parameters['prefix'],
                                                    xstart, xstop,
                                                    ystart, ystop,
                                                    zstart, zstop,
                                                    self.parameters['extension'])
        return os.path.join(self.parameters['root_dir'], filename)


class Hdf5ChunkTileProcessor(TileProcessor):
    """A Tile processor for large slices stored in hdf5 files.

    Assumes the data is stored in a dataset and an optional offset is stored in a dataset

    """

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.fs = None

    def setup(self, parameters):
        """ Method to load the file for uploading

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "upload_format": "<png|tiff>",
                                         "data_name": str,
                                         "z_chunk_size": the chunk extent in the z dimension,
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
        if self.parameters['datatype'] == "uint8":
            datatype = np.uint8
        elif self.parameters['datatype'] == "uint16":
            datatype = np.uint16
        elif self.parameters['datatype'] == "uint32":
            datatype = np.uint32
        else:
            raise Exception("Unsupported datatype: {}".format(self.parameters['datatype']))

        try:
            file_path = self.fs.get_file(file_path)

            # Open hdf5
            h5_file = h5py.File(file_path, 'r')

            # Compute z-index (plugin assumes xy extent fits in a tile)
            z_index = z_index % self.parameters['z_chunk_size']

            # Allocate Tile
            tile_data = np.array(h5_file[self.parameters['data_name']][z_index, :, :], dtype=datatype, order='C')

        except botocore.exceptions.ClientError as err:
            logger = logging.getLogger('ingest-client')
            logger.info("Could not find chunk. Assuming it's missing and generating blank data.")
            # TODO: remove kludge once we have contiguous datasets.
            tile_data = np.zeros((512, 512), dtype=datatype, order="C")
        except OSError as err:
            logger = logging.getLogger('ingest-client')
            logger.info("Could not find chunk. Assuming it's missing and generating blank data.")
            # TODO: remove kludge once we have contiguous datasets.
            tile_data = np.zeros((512, 512), dtype=datatype, order="C")

        upload_img = Image.fromarray(tile_data)
        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output


class Hdf5SingleFilePathProcessor(PathProcessor):
    """A Path processor for 3D datasets stored in a single hdf5 file.

    """
    def __init__(self):
        """Constructor to add custom class var"""
        PathProcessor.__init__(self)

    def setup(self, parameters):
        """Set the params

        MUST HAVE THE CUSTOM PARAMETERS: "filename": "<path_to_filet>",

        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters

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
        # prepend root, append extension
        return self.parameters['filename']


class Hdf5SingleFileTileProcessor(TileProcessor):
    """A Tile processor for 3D datasets stored in a single HDF5 file

    Assumes the data is stored in a dataset and an optional offset is stored in a dataset

    """

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.fs = None

    def setup(self, parameters):
        """ Method to load the file for uploading

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "upload_format": "<png|tiff>",
                                         "data_name": str,
                                         "datatype": <uint8|uint16|uint32>
                                         "offset_x": int,
                                         "offset_y": int,
                                         "offset_z": int,
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

        # Compute global range
        target_x_range = [self.parameters["ingest_job"]["tile_size"]["x"] * x_index,
                          self.parameters["ingest_job"]["tile_size"]["x"] * (x_index + 1)]
        target_y_range = [self.parameters["ingest_job"]["tile_size"]["y"] * y_index,
                          self.parameters["ingest_job"]["tile_size"]["y"] * (y_index + 1)]

        # Open hdf5
        h5_file = h5py.File(file_path, 'r')

        # Compute range in actual data, taking offsets into account
        x_offset = self.parameters['offset_x']
        y_offset = self.parameters['offset_y']
        x_tile_size = self.parameters["ingest_job"]["tile_size"]["x"]
        y_tile_size = self.parameters["ingest_job"]["tile_size"]["y"]

        h5_x_range = [target_x_range[0] + x_offset, target_x_range[1] + x_offset]
        h5_y_range = [target_y_range[0] + y_offset, target_y_range[1] + y_offset]
        h5_z_slice = z_index + self.parameters['offset_z']

        tile_x_range = [0, x_tile_size]
        tile_y_range = [0, y_tile_size]

        h5_max_x = h5_file[self.parameters['data_name']].shape[2]
        h5_max_y = h5_file[self.parameters['data_name']].shape[1]

        if h5_x_range[0] < 0:
            # insert sub-region into tile
            tile_x_range = [h5_x_range[0] * -1, x_tile_size]
            h5_x_range[0] = 0
        if h5_y_range[0] < 0:
            # insert sub-region into tile
            tile_y_range = [h5_y_range[0] * -1, y_tile_size]
            h5_y_range[0] = 0

        if h5_x_range[1] > h5_max_x:
            # insert sub-region into tile
            tile_x_range = [0, x_tile_size - (h5_x_range[1] - h5_max_x)]
            h5_x_range[1] = h5_max_x
        if h5_y_range[1] > h5_max_y:
            # insert sub-region into tile
            tile_y_range = [0, y_tile_size - (h5_y_range[1] - h5_max_y)]
            h5_y_range[1] = h5_max_y

        if self.parameters['datatype'] == "uint8":
            datatype = np.uint8
        elif self.parameters['datatype']== "uint16":
            datatype = np.uint16
        elif self.parameters['datatype']== "uint32":
            datatype = np.uint32
        else:
            raise Exception("Unsupported datatype: {}".format(self.parameters['datatype']))

        # Allocate Tile
        tile_data = np.zeros((self.parameters["ingest_job"]["tile_size"]["y"],
                             self.parameters["ingest_job"]["tile_size"]["x"]),
                             dtype=datatype, order='C')

        if h5_z_slice >= 0:
            # Copy sub-img to tile, save, return
            tile_data[tile_y_range[0]:tile_y_range[1],
                      tile_x_range[0]:tile_x_range[1]] = np.array(h5_file[self.parameters['data_name']][
                                                                          h5_z_slice,
                                                                          h5_y_range[0]:h5_y_range[1],
                                                                          h5_x_range[0]:h5_x_range[1]])

        tile_data = tile_data.astype(datatype)
        upload_img = Image.fromarray(tile_data)

        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["upload_format"].upper())

        # Send handle back
        return output
