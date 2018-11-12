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
from ingestclient.core.validator import Validator, BossValidatorV02
from ingestclient.core.backend import Backend, BossBackend
from ingestclient.core.config import Configuration, ConfigFileError

import os
import unittest
import json
from pkg_resources import resource_filename

class TestValidateConfig(unittest.TestCase):
    def get_skeleton_config(self):
        """
        Returns a partial config that can be adjusted for different tests.

        Returns:
            (dict)
        """
        return {
            "schema": {
              "name": "boss-v0.2-schema",
              "validator": "BossValidatorV02"
            },
            "client": {
                "backend": {
                  "name": "boss",
                  "class": "BossBackend",
                  "host": "api.theboss.io",
                  "protocol": "https"
                },
                "path_processor": {
                  "class": "ingestclient.plugins.cloudvolume.CloudVolumePathProcessor",
                  "params": {
                  }
                }
                #"tile_processor": {}
                #"chunk_processor": {}
            },
            "database": {
                "collection": "my_col_1",
                "experiment": "my_exp_1",
                "channel": "my_ch_1"
            },
            "ingest_job": {
                # "ingest_type": "tile|volumetric",
                "resolution": 0,
                "extent": {
                  "x": [0, 8192],
                  "y": [0, 8192],
                  "z": [0, 500],
                  "t": [0, 1]
                }
            }
        }

    def test_valid_config(self):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.2-schema.json")
        with open(schema_file, 'r') as file_handle:
            schema = json.load(file_handle)

        config_file = os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.2-test.json")
        with open(config_file, 'rt') as example_file:
            config_data = json.load(example_file)

        config = Configuration(config_data)
        validator = config.get_validator()
        validator.schema = schema

        msgs = validator.validate()
        self.assertEqual(0, len(msgs['error']))

    def test_no_chunk_processor(self):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.2-schema.json")
        with open(schema_file, 'r') as file_handle:
            schema = json.load(file_handle)

        config_data = self.get_skeleton_config()
        config_data['ingest_job']['ingest_type'] = 'volumetric'
        config_data['ingest_job']['chunk_size'] = {'x': 1024, 'y': 1024, 'z': 64 }

        config = Configuration(config_data)
        validator = config.get_validator()
        validator.schema = schema

        msgs = validator.validate()
        self.assertEqual(1, len(msgs['error']))
        self.assertRegex(msgs['error'][0], '.*chunk_processor.*')

    def test_no_chunk_size(self):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.2-schema.json")
        with open(schema_file, 'r') as file_handle:
            schema = json.load(file_handle)

        config_data = self.get_skeleton_config()
        config_data['ingest_job']['ingest_type'] = 'volumetric'
        config_data['client']['chunk_processor'] = {
            "class": "ingestclient.plugins.cloudvolume.CloudVolumeChunkProcessor",
            "params": { "cloudpath": "gs://neuroglancer/foo/bar" }
        }

        config = Configuration(config_data)
        validator = config.get_validator()
        validator.schema = schema

        msgs = validator.validate()
        self.assertEqual(1, len(msgs['error']))
        self.assertRegex(msgs['error'][0], '.*chunk_size.*')

    def test_no_tile_processor(self):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.2-schema.json")
        with open(schema_file, 'r') as file_handle:
            schema = json.load(file_handle)

        config_data = self.get_skeleton_config()
        config_data['ingest_job']['ingest_type'] = 'tile'
        config_data['ingest_job']['tile_size'] = {'x': 2048, 'y': 1024, 'z': 32 , 't': 1}

        config = Configuration(config_data)
        validator = config.get_validator()
        validator.schema = schema

        msgs = validator.validate()
        self.assertEqual(1, len(msgs['error']))
        self.assertRegex(msgs['error'][0], '.*tile_processor.*')

    def test_no_tile_size(self):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.2-schema.json")
        with open(schema_file, 'r') as file_handle:
            schema = json.load(file_handle)

        config_data = self.get_skeleton_config()
        config_data['ingest_job']['ingest_type'] = 'tile'
        config_data['client']['tile_processor'] = {
            "class": "ingestclient.plugins.stack.ZindexStackTileProcessor",
            "params": {}
        }

        config = Configuration(config_data)
        validator = config.get_validator()
        validator.schema = schema

        msgs = validator.validate()
        self.assertEqual(1, len(msgs['error']))
        self.assertRegex(msgs['error'][0], '.*tile_size.*')


