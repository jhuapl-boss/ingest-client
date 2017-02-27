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
import jsonschema
import json

from ingestclient.core.validator import Validator, BossValidatorV01
from pkg_resources import resource_filename


class BossValidatorV01TestMixin(object):

    def test_factory(self):
        """Method to test creating an instance from the factory"""
        v = Validator.factory("BossValidatorV01", self.example_config_data)

        assert isinstance(v, BossValidatorV01) is True

    def test_load(self):
        """Method to test creating an instance from the factory"""
        v = BossValidatorV01(self.example_config_data)
        assert v.config["schema"]["name"] == "boss-v0.1-schema"

    def test_validate_schema(self):
        """Method to tests validating a good schema"""
        v = BossValidatorV01(self.example_config_data)
        v.schema = self.schema
        resp = v.validate_schema()

        assert not resp

    def test_validate_bad_schema(self):
        """Method to test validating a bad schema"""

        with open(os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.1-missing-field.json"),
                  'rt') as example_file:
            config_data = json.load(example_file)
        v = BossValidatorV01(config_data)
        v.schema = self.schema
        resp = v.validate_schema()

        assert isinstance(resp, jsonschema.ValidationError)

    def test_validate(self):
        """Method to test validation method"""
        # TODO: Complete after validation fully implemented

        v = BossValidatorV01(self.example_config_data)

        v.schema = self.schema
        result = v.validate()

        assert len(result['info']) == 2
        assert len(result['error']) == 0
        assert len(result['question']) == 0


class TestBossValidatorV01(BossValidatorV01TestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            cls.schema = json.load(file_handle)

        with open(os.path.join(resource_filename("ingestclient", "configs"),
                  "boss-v0.1-time-series-example.json"), 'rt') as example_file:
            cls.example_config_data = json.load(example_file)







