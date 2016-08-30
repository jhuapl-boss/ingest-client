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
import os
import unittest
import jsonschema
import responses
import json

from ingest.core.validator import Validator, BossValidatorV01
from pkg_resources import resource_filename


class BossValidatorV01TestMixin(object):

    def test_factory(self):
        """Method to test creating an instance from the factory"""
        v = Validator.factory("BossValidatorV01", os.path.join(resource_filename("ingest", "schema"),
                                                               "boss-v0.1-time-series-example.json"))

        assert isinstance(v, BossValidatorV01) is True

    def test_load(self):
        """Method to test creating an instance from the factory"""
        v = BossValidatorV01(os.path.join(resource_filename("ingest", "schema"), "boss-v0.1-time-series-example.json"))
        assert v.config["schema"]["version"] == "0.1"
        assert v.config["schema"]["name"] == "boss"

    @responses.activate
    def test_validate_schema(self):
        """Method to test creating an instance from the factory"""
        responses.add(responses.GET, 'https://api.theboss.io/ingest/schema/boss/0.1/',
                      json=self.mock_data, status=200)

        v = BossValidatorV01(os.path.join(resource_filename("ingest", "schema"), "boss-v0.1-time-series-example.json"))
        resp = v.validate_schema()

        assert not resp

    @responses.activate
    def test_validate_bad_schema(self):
        """Method to test creating an instance from the factory"""
        responses.add(responses.GET, 'https://api.theboss.io/ingest/schema/boss/0.1/',
                      json=self.mock_data, status=200)

        v = BossValidatorV01(os.path.join(resource_filename("ingest", "test/data"),
                                          "boss-v0.1-missing-field.json"))
        resp = v.validate_schema()

        assert isinstance(resp, jsonschema.ValidationError)

    @responses.activate
    def test_validate(self):
        """Method to test validation method"""
        # TODO: Complete after validation fully implemented
        responses.add(responses.GET, 'https://api.theboss.io/ingest/schema/boss/0.1/',
                      json=self.mock_data, status=200)

        v = BossValidatorV01(os.path.join(resource_filename("ingest", "schema"),
                                          "boss-v0.1-time-series-example.json"))
        result = v.validate()

        assert len(result['info']) == 2
        assert len(result['error']) == 0
        assert len(result['question']) == 0


class TestBossValidatorV01(BossValidatorV01TestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingest", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            s = json.load(file_handle)
            cls.mock_data = {"schema": json.dumps(s)}







