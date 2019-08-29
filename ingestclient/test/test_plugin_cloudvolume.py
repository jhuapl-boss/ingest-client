# Copyright 2019 The Johns Hopkins University Applied Physics Laboratory
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

from .layer_harness import create_layer, delete_layer, layer_path
from ingestclient.core.config import Configuration
import json
import numpy as np
import os
from pkg_resources import resource_filename
import unittest

class TestCloudVolume(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.vol_name = 'test_vol'
        create_layer((1036, 1026, 78), (0, 0, 0), layer_name=cls.vol_name, dtype=np.uint8)
        cls.config_file = os.path.join(
            resource_filename("ingestclient", "test/data"), "boss-v0.2-cloudvolume.json")

        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        cls.config = Configuration(cls.example_config_data)
        cls.config.load_plugins()

        # Point config at generated CloudVolume.
        cls.config.config_data["client"]["chunk_processor"]["params"]["cloudpath"] = (
            'file://{}{}'.format(layer_path, cls.vol_name))

        cls.chunk_procesor = cls.config.chunk_processor_class
        cls.chunk_procesor.setup(cls.config.get_chunk_processor_params())
        cls.chunk_size = (
            cls.config.config_data["ingest_job"]["chunk_size"]["x"],
            cls.config.config_data["ingest_job"]["chunk_size"]["y"],
            cls.config.config_data["ingest_job"]["chunk_size"]["z"],
            1)  # Time dimension.

    @classmethod
    def teardDownClass(cls):
        # Remove test CloudVolume.
        delete_layer()

    def test_process(self):
        foo = None
        chunk, order = self.chunk_procesor.process(foo, 0, 0, 0)
        self.assertEqual(self.chunk_size, chunk.shape)


    def test_process_chunk_trimmed(self):
        """
        Ensure that a smaller chunk returned when the CloudVolume's extents
        exceeded.
        """
        foo = None
        chunk, order = self.chunk_procesor.process(foo, 1, 1, 1)
        expected = (12, 2, 14, 1)
        self.assertEqual(expected, chunk.shape)
