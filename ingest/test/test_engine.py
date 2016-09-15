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
from ingest.core.engine import Engine
from ingest.core.validator import Validator, BossValidatorV01
from ingest.core.backend import Backend, BossBackend
from ingest.core.config import Configuration
from ingest.test.aws import Setup

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
        responses.add(responses.GET, 'https://api.theboss.io/v0.5/ingest/schema/boss/0.1/',
                      json=self.mock_schema, status=200)

        mocked_repsonse = {"ingest_job_id": 23}
        responses.add(responses.POST, 'https://api.theboss.io/v0.5/ingest/job/',
                      json=mocked_repsonse, status=201)

        mocked_repsonse = {"ingest_job_status": 1,
                           "credentials": {"id":"asdfasdf", "secret": "asdfasdfasdfasdf"},
                           "upload_queue": self.queue_url,
                           "tile_bucket": "test-tile-store"}
        responses.add(responses.GET, 'https://api.theboss.io/v0.5/ingest/job/23',
                      json=mocked_repsonse, status=200)

        responses.add(responses.DELETE, 'https://api.theboss.io/v0.5/ingest/job/23', status=200)


class EngineTestMixin(object):

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

    def test_setup(self):
        """Test setting up the engine - no error should occur"""
        engine = Engine(self.config_file, self.api_token)

        with tempfile.NamedTemporaryFile() as temp_file:
            engine.setup(temp_file.name)

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


class TestEngine(EngineTestMixin, ResponsesMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingest", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            s = json.load(file_handle)
            cls.mock_schema = {"schema": s}

        cls.config_file = os.path.join(resource_filename("ingest", "test/data"), "boss-v0.1-test.json")
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
        cls.aws_creds = {"id": "asdfasdf", "secret": "asdfasdfasdfadsf"}

        # Put some stuff on the task queue
        cls.setup_helper.add_tasks(cls.aws_creds["id"], cls.aws_creds['secret'], cls.queue_url)

    @classmethod
    def tearDownClass(cls):
        # Stop mocking
        cls.setup_helper.stop_mocking()





