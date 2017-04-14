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
from ingestclient.core.backend import BossBackend, Backend
from ingestclient.test.aws import Setup

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
        mocked_repsonse = {"id": 23}
        responses.add(responses.POST, 'https://api.theboss.io/latest/ingest/',
                      json=mocked_repsonse, status=201)

        mocked_repsonse = {"ingest_job": {"id": 23,
                                          "ingest_queue": "https://aws.com/myqueue1",
                                          "upload_queue": self.queue_url,
                                          "status": 1,
                                          "tile_count": 500
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

        status, creds, queue_url, tile_bucket, params, tile_count = b.join(23)

        assert b.queue.url == self.queue_url
        assert status == 1
        assert creds == self.aws_creds
        assert queue_url == self.queue_url
        assert tile_bucket == self.tile_bucket_name
        assert tile_count == 500
        assert 'KVIO_SETTINGS' in params
        assert 'OBJECTIO_CONFIG' in params
        assert 'STATEIO_CONFIG' in params
        assert 'ingest_queue' in params

    def test_delete(self):
        """Test deleting an existing ingest job - mock server response"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        b.cancel(23)

    def test_get_task(self):
        """Test getting a task from the upload queue"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        # Put some stuff on the task queue
        self.setup_helper.add_tasks(self.aws_creds["access_key"], self.aws_creds['secret_key'], self.queue_url, b)

        # Join and get a task
        b.join(23)
        msg_id, rx_handle, msg_body = b.get_task()

        assert isinstance(msg_id, str)
        assert isinstance(rx_handle, str)
        assert msg_body == self.setup_helper.test_msg[0]

        msg_id, rx_handle, msg_body = b.get_task()
        assert isinstance(msg_id, str)
        assert isinstance(rx_handle, str)
        assert msg_body == self.setup_helper.test_msg[1]

    def test_encode_tile_key(self):
        """Test encoding an object key"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        params = {"collection": 1,
                  "experiment": 2,
                  "channel": 3,
                  "resolution": 0,
                  "x_index": 5,
                  "y_index": 6,
                  "z_index": 1,
                  "t_index": 0,
                  }

        proj = [str(params['collection']), str(params['experiment']), str(params['channel'])]
        key = b.encode_tile_key(proj,
                                params['resolution'],
                                params['x_index'],
                                params['y_index'],
                                params['z_index'],
                                params['t_index'],
                                )

        assert key == six.u("03ca58a12ec662954ac12e06517d4269&1&2&3&0&5&6&1&0")

    def test_encode_chunk_key(self):
        """Test encoding an object key"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        params = {"collection": 1,
                  "experiment": 2,
                  "channel": 3,
                  "resolution": 0,
                  "x_index": 5,
                  "y_index": 6,
                  "z_index": 1,
                  "t_index": 0,
                  "num_tiles": 16,
                  }

        proj = [str(params['collection']), str(params['experiment']), str(params['channel'])]
        key = b.encode_chunk_key(params['num_tiles'], proj,
                                 params['resolution'],
                                 params['x_index'],
                                 params['y_index'],
                                 params['z_index'],
                                 params['t_index'],
                                 )

        assert key == six.u("77ff984241a0d6aa443d8724a816866d&16&1&2&3&0&5&6&1&0")

    def test_decode_tile_key(self):
        """Test encoding an object key"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        params = {"collection": 1,
                  "experiment": 2,
                  "channel": 3,
                  "resolution": 0,
                  "x_index": 5,
                  "y_index": 6,
                  "z_index": 1,
                  "t_index": 0,
                  }

        proj = [str(params['collection']), str(params['experiment']), str(params['channel'])]
        key = b.encode_tile_key(proj,
                                params['resolution'],
                                params['x_index'],
                                params['y_index'],
                                params['z_index'],
                                params['t_index'],
                                )

        parts = b.decode_tile_key(key)

        assert parts["collection"] == params['collection']
        assert parts["experiment"] == params['experiment']
        assert parts["channel"] == params['channel']
        assert parts["resolution"] == params['resolution']
        assert parts["x_index"] == params['x_index']
        assert parts["y_index"] == params['y_index']
        assert parts["z_index"] == params['z_index']
        assert parts["t_index"] == params['t_index']

    def test_decode_chunk_key(self):
        """Test encoding an object key"""
        b = BossBackend(self.example_config_data)
        b.setup(self.api_token)

        params = {"collection": 1,
                  "experiment": 2,
                  "channel": 3,
                  "resolution": 0,
                  "x_index": 5,
                  "y_index": 6,
                  "z_index": 1,
                  "t_index": 0,
                  "num_tiles": 0,
                  }

        proj = [str(params['collection']), str(params['experiment']), str(params['channel'])]
        key = b.encode_chunk_key(params['num_tiles'], proj,
                                 params['resolution'],
                                 params['x_index'],
                                 params['y_index'],
                                 params['z_index'],
                                 params['t_index'],
                                 )

        parts = b.decode_chunk_key(key)

        assert parts["collection"] == params['collection']
        assert parts["experiment"] == params['experiment']
        assert parts["channel"] == params['channel']
        assert parts["resolution"] == params['resolution']
        assert parts["x_index"] == params['x_index']
        assert parts["y_index"] == params['y_index']
        assert parts["z_index"] == params['z_index']
        assert parts["t_index"] == params['t_index']
        assert parts["num_tiles"] == params['num_tiles']


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

        cls.queue_url = cls.setup_helper.create_queue("test-queue")

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





