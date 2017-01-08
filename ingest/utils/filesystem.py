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
from abc import ABCMeta, abstractmethod
import boto3
import os
import six
import tempfile


class DynamicFilesystem(object):
    """Class to support converting between things that can look like a filesystem

    Always returns a handle to the file, even if in-memory only.
    """

    def __init__(self, filesystem_type, parameters):
        self.filesystem_type = filesystem_type
        self.parameters = parameters
        self.fs = None

        if self.filesystem_type == "s3":
            self.fs = S3Filesystem(self.parameters)
        elif self.filesystem_type == "s3_copy":
            self.fs = S3CopyTempFilesystem(self.parameters)
        elif self.filesystem_type == "local":
            self.fs = LocalFilesystem(self.parameters)
        else:
            raise ValueError("Invalid filesystem type provied: {}".format(self.filesystem_type))

    def get_file(self, path):
        return self.fs.get_file(path)


class DynamicFilesystemAbsPath(object):
    """Class to support converting between things that can look like a filesystem

    Always returns the absolute path to the file.  Useful for plugins that don't want to load big files, but know
    where they are so other logic can partially load data.
    """

    def __init__(self, filesystem_type, parameters):
        self.filesystem_type = filesystem_type
        self.parameters = parameters
        self.fs = None

        if self.filesystem_type == "s3":
            self.fs = S3CopyTempFilesystemAbsPath(self.parameters)
        elif self.filesystem_type == "local":
            self.fs = LocalFilesystemAbsPath(self.parameters)
        else:
            raise ValueError("Invalid filesystem type provied: {}".format(self.filesystem_type))

    def get_file(self, path):
        return self.fs.get_file(path)


# #############################################
# Handle only filesystems
# #############################################
@six.add_metaclass(ABCMeta)
class BaseFilesystem(object):
    """Base class for implementing filesystem like interfaces"""

    def __init__(self, parameters):
        """

        Args:
            parameters(dict): A dictionary of parameters
        """
        self.parameters = parameters

    @abstractmethod
    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (io.BufferedReader): A file handle for the specified file
        """
        raise NotImplemented


class LocalFilesystem(BaseFilesystem):
    """A normal local filesystem"""

    def __init__(self, parameters):
        """

        Args:
            parameters(dict): The local filesystem doesn't need any special parameters
        """
        BaseFilesystem.__init__(self, parameters)

    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (io.BufferedReader): A file handle for the specified file
        """
        return open(path, mode="rb")


class S3Filesystem(BaseFilesystem):
    """An S3 based filesystem"""

    def __init__(self, parameters):
        """The S3 filesystem uses boto3 under the hood and assumes you have setup your boto3 credentials properly.

        Required parameters:
         "bucket": the name of the bucket to use

        Args:
            parameters(dict): Parameters to configure the S3 filesystem
        """
        BaseFilesystem.__init__(self, parameters)

        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(parameters['bucket'])

    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (io.BufferedReader): A file handle for the specified file
        """
        output = six.BytesIO()
        self.bucket.download_fileobj(path, output)
        return output


class S3CopyTempFilesystem(BaseFilesystem):
    """An S3 based filesystem that copies data locally.
    Useful when chunking big tiles, but must have enough local storage"""

    def __init__(self, parameters):
        """The S3 filesystem uses boto3 under the hood and assumes you have setup your boto3 credentials properly.

        Required parameters:
         "bucket": the name of the bucket to use

        Args:
            parameters(dict): Parameters to configure the S3 filesystem
        """
        BaseFilesystem.__init__(self, parameters)

        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(parameters['bucket'])
        self.file_map = {}

    def __del__(self):
        """When the class goes out of scope, clean up all temporary files"""
        for path in self.file_map:
            os.remove(self.file_map[path])

    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (io.BufferedReader): A file handle for the specified file
        """
        if path in self.file_map:
            temp_path = self.file_map[path]
        else:
            # File currently doesn't exist locally.  Download it
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                self.bucket.download_file(path, tmp.name)
            self.file_map[path] = tmp.name
            temp_path = tmp.name

        return open(temp_path, 'rb')


# #############################################
# Path only filesystems
# #############################################


@six.add_metaclass(ABCMeta)
class BaseFilesystemAbsPath(object):
    """Base class for implementing filesystem like interfaces that return paths only"""

    def __init__(self, parameters):
        """

        Args:
            parameters(dict): A dictionary of parameters
        """
        self.parameters = parameters

    @abstractmethod
    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (str): A file handle for the specified file
        """
        raise NotImplemented


class LocalFilesystemAbsPath(BaseFilesystem):
    """A normal local filesystem"""

    def __init__(self, parameters):
        """

        Args:
            parameters(dict): The local filesystem doesn't need any special parameters
        """
        BaseFilesystem.__init__(self, parameters)

    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (str: A file handle for the specified file
        """
        return path


class S3CopyTempFilesystemAbsPath(BaseFilesystem):
    """A version of an S3 Filesystem that copies files to temp space locally, once, to improve performance"""

    def __init__(self, parameters):
        """The S3 filesystem uses boto3 under the hood and assumes you have setup your boto3 credentials properly.

        Required parameters:
         "bucket": the name of the bucket to use

        Args:
            parameters(dict): Parameters to configure the S3 filesystem
        """
        BaseFilesystem.__init__(self, parameters)

        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(parameters['bucket'])
        self.file_map = {}

        if "temp_dir" in parameters:
            self.temp_dir = parameters['temp_dir']
        else:
            self.temp_dir = None

    def __del__(self):
        """When the class goes out of scope, clean up all temporary files"""
        for path in self.file_map:
            os.remove(self.file_map[path])

    def get_file(self, path):
        """Method to get a file from the "file system"

        Args:
            path (str): Path to the file to load

        Returns:
            (io.BufferedReader): A file handle for the specified file
        """
        if path in self.file_map:
            return self.file_map[path]

        # File currently doesn't exist locally.  Download it
        with tempfile.NamedTemporaryFile(delete=False, dir=self.temp_dir) as tmp:
            self.bucket.download_file(path, tmp.name)
        self.file_map[path] = tmp.name

        return tmp.name
