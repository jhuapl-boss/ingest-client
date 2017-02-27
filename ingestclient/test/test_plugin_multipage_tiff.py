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

from ingestclient.core.config import Configuration
from ingestclient.plugins.multipage_tiff import load_tiff_multipage


class TestSingleMultipageTiff(unittest.TestCase):

    def test_SingleTimeTiffPathProcessor_setup(self):
        """Test setting up the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        assert pp.parameters["z_0"] == os.path.join(resource_filename("ingestclient", "test/data"),
                                                    "test_multipage.tif")
        assert pp.parameters["ingest_job"]["extent"]["x"] == [0, 512]

    def test_SingleTimeTiffPathProcessor_process(self):
        """Test running the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())
        assert pp.process(0, 0, 0, 0) == os.path.join(resource_filename("ingestclient", "test/data"),
                                                      "test_multipage.tif")

    def test_SingleTimeTiffPathProcessor_process_invalid(self):
        """Test running the path processor with invalid tile indices"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        with self.assertRaises(IndexError):
            pp.process(1, 0, 0, 0)

        with self.assertRaises(IndexError):
            pp.process(0, 1, 0, 0)

        with self.assertRaises(IndexError):
            pp.process(0, 0, 1, 0)

        with self.assertRaises(IndexError):
            pp.process(0, 0, 0, 11)

    def test_SingleTimeTiffTileProcessor_setup(self):
        """Test setting up the tile processor"""
        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        assert tp.parameters["datatype"] == "uint16"
        assert tp.parameters["ingest_job"]["extent"]["y"] == [0, 256]

    def test_SingleTimeTiffTileProcessor_process(self):
        """Test running the tile processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        filename = pp.process(0, 0, 0, 0)
        handle = tp.process(filename, 0, 0, 0, 3)

        # Open handle as image file
        test_img = Image.open(handle)
        test_img = np.array(test_img, dtype="uint16")

        # Open original data
        truth_img = load_tiff_multipage(filename)
        truth_img = np.array(truth_img, dtype="uint16")
        truth_img = truth_img[3, :, :]

        # Make sure the same
        np.testing.assert_array_equal(truth_img, test_img)

    @classmethod
    def setUpClass(cls):
        cls.config_file = os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.1-singleMultipageTiff.json")

        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # inject the file path since we don't want to hardcode
        cls.example_config_data["client"]["path_processor"]["params"]["z_0"] = os.path.join(resource_filename("ingestclient",
                                                                                                              "test/data"),
                                                                                                              "test_multipage.tif")

        cls.config = Configuration(cls.example_config_data)
        cls.config.load_plugins()







