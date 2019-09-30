# Copyright 2019 The Johns Hopkins University Applied Physics Laboratory
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
from ingestclient.core.backend import BossBackend, Backend
from ingestclient.test.aws import Setup

import boto3
import os
import unittest
import json
import responses
from pkg_resources import resource_filename
import six
import mock

ERROR_TEXT = "Error on the server"

class ResponsesMixin(object):
    """Mixin to setup requests mocking for the test class"""
    def setUp(self):
        responses._default_mock.__enter__()
        self.add_default_response()
        super(ResponsesMixin, self).setUp()

    def tearDown(self):
        super(ResponsesMixin, self).tearDown()
        responses._default_mock.__exit__(None, None, None)

    def add_default_response(self):
        mocked_repsonse = {"id": 23}

        mocked_repsonse = {"text": ERROR_TEXT}
        responses.add(responses.GET, 'https://api.theboss.io/latest/ingest/23',
                      json=mocked_repsonse, status=500)
        responses.add(responses.GET, 'https://api.theboss.io/latest/ingest/23/status',
                      json=mocked_repsonse, status=500)




class BossBackendTestMixin(object):


    @mock.patch('time.sleep')
    def test_join_retry(self, fake_sleep):
        """Test creating an ingest job - mock server response"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        with self.assertRaises(Exception) as context:
            status, creds, queue_url, tile_index_queue_url, tile_bucket, params, tile_count = b.join(23)


    @mock.patch('time.sleep')
    def test_get_status_retry(self, fake_sleep):
        """Test creating an ingest job - mock server response"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        with self.assertRaises(Exception) as context:
            b.get_job_status(23)



class TestBossBackend(BossBackendTestMixin, ResponsesMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingestclient", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            s = json.load(file_handle)
            cls.mock_schema = {"schema": s}

        with open(os.path.join(resource_filename("ingestclient", "configs"),
                  "boss-v0.1-time-series-example.json"), 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # Setup AWS stuff
        cls.setup_helper = Setup()
        cls.setup_helper.mock = True
        cls.setup_helper.start_mocking()

        queue_names = ["test-queue", "test-index-queue"]
        cls.upload_queue_url, cls.tile_index_queue_url = cls.setup_helper.create_queue(queue_names)

        cls.tile_bucket_name = "test-tile-store"
        cls.setup_helper.create_bucket("test-tile-store")

        # mock api token
        cls.api_token = "aalasdklbajklsbfasdklbfkjdsb"

        # mock aws creds
        cls.aws_creds = {"access_key": "1234", "secret_key": "asdfasdfasdfadsf"}

    @classmethod
    def tearDownClass(cls):
        # Stop mocking
        cls.setup_helper.stop_mocking()





