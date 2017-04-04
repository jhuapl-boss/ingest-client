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
import tempfile
import responses

try:
    import mock
except ImportError:
    from unittest import mock

from ingestclient.core.config import Configuration, ConfigPropertyObject, BossConfigurationGenerator
from ingestclient.core.validator import BossValidatorV01
from ingestclient.core.backend import BossBackend
from ingestclient.plugins.path import TestPathProcessor
from ingestclient.plugins.tile import TestTileProcessor

from pkg_resources import resource_filename


def token_name_side_effect():
    return {"INTERN_TOKEN": "adlsfjadsf"}


class TestConfigPropertyObject(unittest.TestCase):

    def test_basic(self):
        """Method to test the ConfigPropertyObject class"""
        a = ConfigPropertyObject("propA", {"a": 2, "b": "asdf"},  {"a": "This is an A thing", "b": "This is a B thing"})
        assert a.a == 2
        assert a.b == "asdf"

    def test_helpers(self):
        """Method to test the ConfigPropertyObject class"""
        a = ConfigPropertyObject("propA", {"a": 2, "b": "asdf"}, {"a": "This is an A thing", "b": "This is a B thing"})

        props = a.get_properties()
        assert len(props) == 2
        assert "a" in props
        assert "b" in props

        assert a.get_name() == "propA"

        assert a.get_help_str("a") == "This is an A thing"
        assert a.get_help_str("b") == "This is a B thing"

    def test_adding_property(self):
        """Method to test the ConfigPropertyObject class"""
        a = ConfigPropertyObject("propA", {"a": 2, "b": "asdf"}, {"a": "This is an A thing", "b": "This is a B thing"})
        a.add_property("c", [2, 3, 4], "this is a C thing")
        props = a.get_properties()
        assert len(props) == 3
        assert "a" in props
        assert "b" in props
        assert "c" in props

        assert a.get_help_str("a") == "This is an A thing"
        assert a.get_help_str("b") == "This is a B thing"
        assert a.get_help_str("c") == "this is a C thing"

        assert a.c == [2, 3, 4]

    def test_to_dict_basic(self):
        """Method to test the ConfigPropertyObject class"""
        a = ConfigPropertyObject("propA", {"a": 2, "b": "asdf"}, {"a": "This is an A thing", "b": "This is a B thing"})
        props = a.to_dict()

        assert props["propA"]["a"] == 2
        assert props["propA"]["b"] == "asdf"

    def test_adding_property_object(self):
        """Method to test the ConfigPropertyObject class"""
        cpo = ConfigPropertyObject("client", {"a": 2, "b": "asdf"}, "This is a thing")
        cpo.add_property_object(ConfigPropertyObject("intro", {"a": 5, "b": "fdsa"}, "This is a thing 2"))
        assert cpo.a == 2
        assert cpo.b == "asdf"
        assert cpo.intro.a == 5
        assert cpo.intro.b == "fdsa"

    def test_to_dict_compound(self):
        """Method to test the ConfigPropertyObject class"""
        cpo = ConfigPropertyObject("client", {"a": 2, "b": "asdf"}, "This is a thing")
        cpo.add_property_object(ConfigPropertyObject("intro", {"a": 5, "b": "fdsa"}, "This is a thing 2"))
        props = cpo.to_dict()

        assert props["client"]["a"] == 2
        assert props["client"]["b"] == "asdf"
        assert props["client"]["intro"]["a"] == 5
        assert props["client"]["intro"]["b"] == "fdsa"


class TestBossConfigurationGenerator(unittest.TestCase):

    def test_create(self):
        """Method to test the BossConfigurationGenerator class"""
        bcg = BossConfigurationGenerator()
        bcg.explain()  # Just make sure an error doesn't occur
        assert bcg.description == "Configuration Generator for the Boss ingest service v0.1"
        assert bcg.name == "Boss Ingest v0.1"

    def test_save_load(self):
        """Method to test the BossConfigurationGenerator class"""
        bcg = BossConfigurationGenerator()
        with tempfile.NamedTemporaryFile(suffix=".pickle") as temp:
            bcg.save(temp.name)

            # Load it into a new object
            bcg2 = BossConfigurationGenerator()
            bcg2.load(temp.name)

            # Make sure they are the same
            assert bcg.__dict__ == bcg.__dict__

    def test_to_json(self):
        """Method to test json serialization"""
        bcg = BossConfigurationGenerator()
        with tempfile.NamedTemporaryFile(suffix=".json") as temp:
            bcg.to_json(temp.name)

            with open(temp.name, 'rt') as json_file:
                data = json.load(json_file)

            # Make sure it serialized properly
            assert "client" in data
            assert "schema" in data
            assert "backend" in data['client']
            assert "file_processor" in data['client']
            assert "tile_processor" in data['client']
            assert "class" in data['client']['file_processor']
            self.assertEqual(data["schema"]["name"], "boss")
            self.assertEqual(data["schema"]["validator"], "BossValidatorV01")


class ConfigurationTestMixin(object):

    def test_create(self):
        """Test creating a Configuration object"""
        config = Configuration(self.example_config_data)
        config.load_plugins()

        assert isinstance(config, Configuration)
        assert isinstance(config.tile_processor_class, TestTileProcessor)
        assert isinstance(config.path_processor_class, TestPathProcessor)

    def test_to_json(self):
        """Test json serialization"""
        config = Configuration(self.example_config_data)
        config.load_plugins()

        json_data = config.to_json()

        json_dict = json.loads(json_data)

        assert json_dict == self.example_config_data

    def test_get_validator(self):
        """Test dynamically getting the validator class"""
        config = Configuration(self.example_config_data)
        config.load_plugins()

        v = config.get_validator()

        assert isinstance(v, BossValidatorV01)


class TestConfiguration(ConfigurationTestMixin, unittest.TestCase):

    @responses.activate
    @mock.patch.dict(os.environ, {"INTERN_TOKEN": "adlsfjadsf"})
    def test_get_backend_env_var(self):
        """Test dynamically getting the validator class"""
        config = Configuration(self.example_config_data)

        b = config.get_backend()
        b.setup()

        assert isinstance(b, BossBackend)

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            cls.schema = json.load(file_handle)

        cls.config_file = os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.1-test.json")

        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # Mock api token since you can't bank on ndio being configured
        cls.api_token = "adlsfjadsf"







