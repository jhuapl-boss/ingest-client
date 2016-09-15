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
from ingest.core.backend import BossBackend, Backend
from ingest.test.aws import Setup

import os
import unittest
import json
import responses
from pkg_resources import resource_filename
import six


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


class BossBackendTestMixin(object):

    def test_factory(self):
        """Method to test creating an instance from the factory"""
        b = Backend.factory("BossBackend", self.example_config_data)

        assert isinstance(b, BossBackend) is True

    def test_setup(self):
        """Method to test setup instance"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        assert b.host == "https://api.theboss.io"

    def test_setup_upload_queue(self):
        """Test connecting the backend to the upload queue"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        b.setup_upload_queue(self.aws_creds, self.queue_url)

        assert b.queue.url == self.queue_url

    def test_create(self):
        """Test creating an ingest job - mock server response"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        id = b.create(self.example_config_data)

        assert id == 23

    def test_join(self):
        """Test joining an existing ingest job - mock server response"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        status, creds, queue_url, tile_bucket = b.join(23)

        assert b.queue.url == self.queue_url
        assert status == 1
        assert isinstance(creds, dict)
        assert queue_url == self.queue_url
        assert tile_bucket == self.tile_bucket_name

    def test_delete(self):
        """Test deleting an existing ingest job - mock server response"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        b.cancel(23)

    def test_get_task(self):
        """Test getting a task from the upload queue"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        b.join(23)

        msg_id, rx_handle, msg_body = b.get_task()

        assert isinstance(msg_id, str)
        assert isinstance(rx_handle, str)

        assert msg_body == self.setup_helper.test_msg

    def test_encode_object_key(self):
        """Test encoding an object key"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        b.join(23)

        msg_id, rx_handle, msg_body = b.get_task()

        proj = [str(msg_body['collection']), str(msg_body['experiment']), str(msg_body['channel'])]
        key = b.encode_object_key(proj,
                                  msg_body['resolution'],
                                  msg_body['x_tile'],
                                  msg_body['y_tile'],
                                  msg_body['z_tile'],
                                  msg_body['time_sample'],
                                  )

        assert key == six.u("03ca58a12ec662954ac12e06517d4269&1&2&3&0&5&6&1&0")


class TestBossBackend(BossBackendTestMixin, ResponsesMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(resource_filename("ingest", "schema"), "boss-v0.1-schema.json")
        with open(schema_file, 'r') as file_handle:
            s = json.load(file_handle)
            cls.mock_schema = {"schema": s}

        with open(os.path.join(resource_filename("ingest", "configs"),
                  "boss-v0.1-time-series-example.json"), 'rt') as example_file:
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





