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
from ingestclient.core.engine import Engine
from ingestclient.core.validator import Validator, BossValidatorV01
from ingestclient.core.backend import Backend, BossBackend
from ingestclient.core.config import Configuration, ConfigFileError
from ingestclient.test.aws import Setup

import os
import unittest
import json
import responses
from pkg_resources import resource_filename
import tempfile
import boto3


class ResponsesMixin(object):
    """Mixin to setup requests mocking for the test class"""
    def setUp(self):
        responses._default_mock.__enter__()
        self.add_default_response()
        super(ResponsesMixin, self).setUp()

    def tearDown(self):
        super(ResponsesMixin, self).tearDown()
        responses._default_mock.__exit__()

    def add_default_response(self):
        mocked_repsonse = {"id": 23}
        responses.add(responses.POST, 'https://api.theboss.io/latest/ingest/',
                      json=mocked_repsonse, status=201)

        mocked_repsonse = {"ingest_job": {"id": 23,
                                          "ingest_queue": "https://aws.com/myqueue1",
                                          "upload_queue": self.queue_url,
                                          "status": 1,
                                          "tile_count": 500,
                                          },
                           "ingest_lambda": "my_lambda",
                           "tile_bucket_name": self.tile_bucket_name,
                           "KVIO_SETTINGS": {"settings": "go here"},
                           "STATEIO_CONFIG": {"settings": "go here"},
                           "OBJECTIO_CONFIG": {"settings": "go here"},
                           "credentials": self.aws_creds,
                           "resource": {"resource": "stuff"}
                           }
        responses.add(responses.GET, 'https://api.theboss.io/latest/ingest/23',
                      json=mocked_repsonse, status=200)

        responses.add(responses.DELETE, 'https://api.theboss.io/latest/ingest/23', status=204)



class EngineBossTestMixin(object):

    def test_create_instance(self):
        """Method to test creating an instance from the factory"""
        engine = Engine(self.config_file, self.api_token)

        assert isinstance(engine, Engine) is True
        assert isinstance(engine.backend, Backend) is True
        assert isinstance(engine.backend, BossBackend) is True
        assert isinstance(engine.validator, Validator) is True
        assert isinstance(engine.validator, BossValidatorV01) is True
        assert isinstance(engine.config, Configuration) is True

        # Schema loaded
        assert isinstance(engine.config.schema, dict) is True
        assert engine.config.schema["type"] == "object"

    def test_missing_file(self):
        """Test creating a Configuration object"""
        with self.assertRaises(ConfigFileError):
            engine = Engine("/asdfhdfgkjldhsfg.json", self.api_token)

    def test_bad_file(self):
        """Test creating a Configuration object"""
        with tempfile.NamedTemporaryFile(suffix='.json') as test_file:
            with open(test_file.name, 'wt') as test_file_handle:
                test_file_handle.write("garbage garbage garbage\n")

            with self.assertRaises(ConfigFileError):
                engine = Engine(test_file.name, self.api_token)

    def test_setup(self):
        """Test setting up the engine - no error should occur"""
        engine = Engine(self.config_file, self.api_token)
        engine.setup()

    def test_create_job(self):
        """Test creating an ingest job - mock server response"""
        engine = Engine(self.config_file, self.api_token)

        engine.create_job()

        assert engine.ingest_job_id == 23

    def test_join(self):
        """Test joining an existing ingest job - mock server response"""
        engine = Engine(self.config_file, self.api_token, 23)

        engine.join()

        assert engine.upload_job_queue == self.queue_url
        assert engine.job_status == 1

    def test_run(self):
        """Test getting a task from the upload queue"""
        engine = Engine(self.config_file, self.api_token, 23)
        engine.msg_wait_iterations = 2

        # Put some stuff on the task queue
        self.setup_helper.add_tasks(self.aws_creds["access_key"], self.aws_creds['secret_key'], self.queue_url, engine.backend)

        engine.join()
        engine.run()

        # Check for tile to exist
        s3 = boto3.resource('s3')
        tile_bucket = s3.Bucket(self.tile_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as data:
                tile_bucket.download_fileobj("03ca58a12ec662954ac12e06517d4269&1&2&3&0&5&6&1&0", data)

                # Make sure the key was valid an data was loaded into the file handles
                assert data.tell() == 182300


class TestBossEngine(EngineBossTestMixin, ResponsesMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            s = json.load(file_handle)
            cls.mock_schema = {"schema": s}

        cls.config_file = os.path.join(resource_filename("ingestclient", "test/data"), "boss-v0.1-test.json")
        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # Setup AWS stuff
        cls.setup_helper = Setup()
        cls.setup_helper.mock = True
        cls.setup_helper.start_mocking()

        cls.queue_url = cls.setup_helper.create_queue("test-queue")

        cls.tile_bucket_name = "test-tile-store"
        cls.setup_helper.create_bucket("test-tile-store")

        # mock api token
        cls.api_token = "aalasdklbajklsbfasdklbfkjdsb"

        # mock aws creds
        cls.aws_creds = {"access_key": "asdfasdf", "secret_key": "asdfasdfasdfadsf"}

    @classmethod
    def tearDownClass(cls):
        # Stop mocking
        cls.setup_helper.stop_mocking()





