# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import

import os
import unittest
import json
from pkg_resources import resource_filename

from PIL import Image
import numpy as np

from moto import mock_s3
import boto3

from ingestclient.core.config import Configuration


class ZImageStackMixin(object):

    def test_PathProcessor_process(self):
        """Test running the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())
        assert pp.process(0, 0, 0, 0) == "{}/3253_my_stack_section000.png".format(pp.parameters["root_dir"])
        assert pp.process(0, 0, 1, 0) == "{}/3254_my_stack_section001.png".format(pp.parameters["root_dir"])

    def test_PathProcessor_process_invalid(self):
        """Test running the path processor with invalid tile indices"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        with self.assertRaises(IndexError):
            pp.process(0, 0, 3, 0)

        with self.assertRaises(IndexError):
            pp.process(0, 0, 0, 1)

    def test_TileProcessor_process(self):
        """Test running the tile processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        filename = pp.process(0, 0, 0, 0)
        handle = tp.process(filename, 0, 0, 0, 0)

        # Open handle as image file
        test_img = Image.open(handle)
        test_img = np.array(test_img, dtype="uint8")

        # Open original data
        truth_file = os.path.join(resource_filename("ingestclient", "test/data/example_z_stack/"), "3253_my_stack_section000.png")
        truth_img = Image.open(truth_file)
        truth_img = np.array(truth_img, dtype="uint8")

        # Make sure the same
        np.testing.assert_array_equal(truth_img, test_img)


class TestZImageStackLocal(ZImageStackMixin, unittest.TestCase):

    def test_PathProcessor_setup(self):
        """Test setting up the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        assert pp.parameters["root_dir"] == resource_filename("ingestclient", "test/data/example_z_stack")
        assert pp.parameters["ingest_job"]["extent"]["y"] == [0, 512]

    def test_TileProcessor_setup(self):
        """Test setting up the tile processor"""
        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        assert tp.parameters["extension"] == "png"
        assert tp.parameters["filesystem"] == "local"
        assert tp.parameters["ingest_job"]["extent"]["y"] == [0, 512]

    @classmethod
    def setUpClass(cls):
        cls.config_file = os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.1-zStack.json")

        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # inject the file path since we don't want to hardcode
        cls.example_config_data["client"]["path_processor"]["params"]["root_dir"] = resource_filename("ingestclient",
                                                                                                      "test/data/example_z_stack")

        cls.config = Configuration(cls.example_config_data)
        cls.config.load_plugins()


class TestZImageStackS3(ZImageStackMixin, unittest.TestCase):
    mock_s3 = None

    def test_PathProcessor_setup(self):
        """Test setting up the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        assert pp.parameters["root_dir"] == "example_z_stack"
        assert pp.parameters["ingest_job"]["extent"]["y"] == [0, 512]

    def test_TileProcessor_setup(self):
        """Test setting up the tile processor"""
        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        assert tp.parameters["extension"] == "png"
        assert tp.parameters["filesystem"] == "s3"
        assert tp.parameters["bucket"] == "my_bucket"
        assert tp.parameters["ingest_job"]["extent"]["y"] == [0, 512]

    @classmethod
    def setUpClass(cls):
        cls.config_file = os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.1-zStack.json")

        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # inject the file path since we don't want to hardcode
        cls.example_config_data["client"]["path_processor"]["params"]["root_dir"] = "example_z_stack"

        # Switch to S3
        cls.example_config_data["client"]["tile_processor"]["params"]["filesystem"] = "s3"
        cls.example_config_data["client"]["tile_processor"]["params"]["bucket"] = "my_bucket"

        cls.config = Configuration(cls.example_config_data)
        cls.config.load_plugins()

        # Set up bucket
        cls.mock_s3 = mock_s3()
        cls.mock_s3.start()

        client = boto3.client('s3', region_name="us-east-1")
        _ = client.create_bucket(ACL='private',
                                 Bucket=cls.example_config_data["client"]["tile_processor"]["params"]["bucket"])
        waiter = client.get_waiter('bucket_exists')
        waiter.wait(Bucket=cls.example_config_data["client"]["tile_processor"]["params"]["bucket"])

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cls.example_config_data["client"]["tile_processor"]["params"]["bucket"])

        # Put images in S3
        imgs = [os.path.join(resource_filename("ingestclient", "test/data/example_z_stack"), "3253_my_stack_section000.png"),
                os.path.join(resource_filename("ingestclient", "test/data/example_z_stack"), "3254_my_stack_section001.png")]

        keys = ["example_z_stack/3253_my_stack_section000.png",
                "example_z_stack/3254_my_stack_section001.png"]
        for key, img in zip(keys, imgs):
            # put file
            bucket.upload_file(img, key)

    @classmethod
    def tearDownClass(cls):
        cls.mock_s3.stop()








