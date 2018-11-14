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
from ingestclient.core.validator import Validator, BossValidatorV02
from ingestclient.core.backend import Backend, BossBackend
from ingestclient.core.config import Configuration, ConfigFileError
from ingestclient.core.consts import BOSS_CUBOID_X, BOSS_CUBOID_Y, BOSS_CUBOID_Z
from ingestclient.test.aws import Setup, VOLUMETRIC_CUBOID_KEY, VOLUMETRIC_CHUNK_KEY
from ingestclient.plugins.chunk import XYZ_ORDER, ZYX_ORDER, XYZT_ORDER, TZYX_ORDER

import os
import unittest

import sys
#This was added mainly to support python 2.7 testing as well
if sys.version_info >= (3, 3):
    #python 3
    from unittest.mock import MagicMock
else:
    #python 2
    from mock import MagicMock

import json
import responses
from pkg_resources import resource_filename
import tempfile
import boto3
import blosc
import numpy as np


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
        mocked_response = {"id": 23}
        responses.add(
            responses.POST,
            'https://api.theboss.io/latest/ingest/',
            json=mocked_response,
            status=201)

        mocked_response = {
            "ingest_job": {
                "id": 23,
                "ingest_queue": "https://aws.com/myqueue1",
                "upload_queue": self.queue_url,
                "status": 1,
                "tile_count": 500,
            },
            "ingest_lambda": "my_lambda",
            "tile_bucket_name": self.tile_bucket_name,
            "ingest_bucket_name": self.ingest_bucket_name,
            "KVIO_SETTINGS": {
                "settings": "go here"
            },
            "STATEIO_CONFIG": {
                "settings": "go here"
            },
            "OBJECTIO_CONFIG": {
                "settings": "go here"
            },
            "credentials": self.aws_creds,
            "resource": {
                "resource": "stuff"
            }
        }
        responses.add(
            responses.GET,
            'https://api.theboss.io/latest/ingest/23',
            json=mocked_response,
            status=200)

        responses.add(
            responses.DELETE,
            'https://api.theboss.io/latest/ingest/23',
            status=204)


class EngineBossTestMixin(object):
    def test_create_instance(self):
        """Method to test creating an instance from the factory"""
        engine = Engine(self.config_file, self.api_token)

        assert isinstance(engine, Engine) is True
        assert isinstance(engine.backend, Backend) is True
        assert isinstance(engine.backend, BossBackend) is True
        assert isinstance(engine.validator, Validator) is True
        assert isinstance(engine.validator, BossValidatorV02) is True
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
        engine.msg_wait_iterations = 0

        # Put some stuff on the task queue
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()
        engine.run()

        # Check for tile to exist
        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # Using an empty CloudVolume dataset so all values should be 0.
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                unique_vals = np.unique(cuboid)
                assert 1 == len(unique_vals)
                assert 0 == unique_vals[0]

    def test_upload_cuboid_indexing(self):
        data = np.random.randint(
            0, 256, (BOSS_CUBOID_X, BOSS_CUBOID_Y, BOSS_CUBOID_Z), 'uint8')
        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = data

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        x = 1024
        y = 512
        z = 16
        assert True == engine.upload_cuboid(chunk, x, y, z,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, XYZ_ORDER)

        exp_x = slice(x, x + BOSS_CUBOID_X, None)
        exp_y = slice(y, y + BOSS_CUBOID_Y, None)
        exp_z = slice(z, z + BOSS_CUBOID_Z, None)

        chunk.__getitem__.assert_called_with((exp_x, exp_y, exp_z))

    def test_upload_cuboid_random_data_xyzt_order(self):
        data = np.random.randint(
            0, 256, (BOSS_CUBOID_X, BOSS_CUBOID_Y, BOSS_CUBOID_Z, 1), 'uint8')
        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = data

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 2048, 32,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, XYZT_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert np.array_equal(np.transpose(data), cuboid)

    def test_upload_cuboid_random_data_tzyx_order(self):
        data = np.random.randint(
            0, 256, (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y, BOSS_CUBOID_X), 'uint8')
        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = data

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 2048, 32,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, TZYX_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert np.array_equal(data, cuboid)

    def test_upload_cuboid_random_data_xyz_order(self):
        data = np.random.randint(
            0, 256, (BOSS_CUBOID_X, BOSS_CUBOID_Y, BOSS_CUBOID_Z), 'uint8')
        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = data

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 512, 48,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, XYZ_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert np.array_equal(
                    np.expand_dims(np.transpose(data), 0), cuboid)

    def test_upload_cuboid_random_data_zyx_order(self):
        data = np.random.randint(
            0, 256, (BOSS_CUBOID_Z, BOSS_CUBOID_Y, BOSS_CUBOID_X), 'uint8')
        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = data

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 512, 48,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, ZYX_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert np.array_equal(np.expand_dims(data, 0), cuboid)

    def test_upload_cuboid_partial_cuboid_zyx_order(self):
        missing_z = 3
        z_stop = BOSS_CUBOID_Z - missing_z
        missing_y = 11
        y_stop = BOSS_CUBOID_Y - missing_y
        missing_x = 7
        x_stop = BOSS_CUBOID_X - missing_x
        partial_cuboid = np.random.randint(0, 256, (z_stop, y_stop, x_stop),
                                           'uint8')

        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = partial_cuboid

        expected_cuboid = np.pad(
            np.expand_dims(partial_cuboid, 0),
            ((0, 0), (0, missing_z), (0, missing_y), (0, missing_x)),
            'constant',
            constant_values=0)

        assert expected_cuboid.shape == (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y,
                                         BOSS_CUBOID_X)

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 512, 48,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, ZYX_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert expected_cuboid.shape == cuboid.shape
                assert np.array_equal(expected_cuboid, cuboid)

    def test_upload_cuboid_partial_cuboid_xyz_order(self):
        missing_z = 3
        z_stop = BOSS_CUBOID_Z - missing_z
        missing_y = 11
        y_stop = BOSS_CUBOID_Y - missing_y
        missing_x = 7
        x_stop = BOSS_CUBOID_X - missing_x
        partial_cuboid = np.random.randint(0, 256, (x_stop, y_stop, z_stop),
                                           'uint8')

        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = partial_cuboid

        expected_cuboid = np.pad(
            np.expand_dims(np.transpose(partial_cuboid), 0),
            ((0, 0), (0, missing_z), (0, missing_y), (0, missing_x)),
            'constant',
            constant_values=0)

        assert expected_cuboid.shape == (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y,
                                         BOSS_CUBOID_X)

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 512, 48,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, XYZ_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert expected_cuboid.shape == cuboid.shape
                assert np.array_equal(expected_cuboid, cuboid)

    def test_upload_cuboid_partial_cuboid_tzyx_order(self):
        missing_z = 3
        z_stop = BOSS_CUBOID_Z - missing_z
        missing_y = 11
        y_stop = BOSS_CUBOID_Y - missing_y
        missing_x = 7
        x_stop = BOSS_CUBOID_X - missing_x
        partial_cuboid = np.random.randint(0, 256, (1, z_stop, y_stop, x_stop),
                                           'uint8')

        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = partial_cuboid

        expected_cuboid = np.pad(
            partial_cuboid, ((0, 0), (0, missing_z), (0, missing_y),
                             (0, missing_x)),
            'constant',
            constant_values=0)

        assert expected_cuboid.shape == (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y,
                                         BOSS_CUBOID_X)

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 512, 48,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, TZYX_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert expected_cuboid.shape == cuboid.shape
                assert np.array_equal(expected_cuboid, cuboid)

    def test_upload_cuboid_partial_cuboid_xyzt_order(self):
        missing_z = 3
        z_stop = BOSS_CUBOID_Z - missing_z
        missing_y = 11
        y_stop = BOSS_CUBOID_Y - missing_y
        missing_x = 7
        x_stop = BOSS_CUBOID_X - missing_x
        partial_cuboid = np.random.randint(0, 256, (x_stop, y_stop, z_stop, 1),
                                           'uint8')

        chunk = MagicMock(spec=np.ndarray)
        chunk.__getitem__.return_value = partial_cuboid

        expected_cuboid = np.pad(
            np.transpose(partial_cuboid), ((0, 0), (0, missing_z),
                                           (0, missing_y), (0, missing_x)),
            'constant',
            constant_values=0)

        assert expected_cuboid.shape == (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y,
                                         BOSS_CUBOID_X)

        engine = Engine(self.config_file, self.api_token, 23)
        self.setup_helper.add_volumetric_tasks(self.aws_creds["access_key"],
                                               self.aws_creds['secret_key'],
                                               self.queue_url, engine.backend)

        engine.join()

        assert True == engine.upload_cuboid(chunk, 1024, 512, 48,
                                            VOLUMETRIC_CUBOID_KEY,
                                            VOLUMETRIC_CHUNK_KEY, XYZT_ORDER)

        s3 = boto3.resource('s3')
        ingest_bucket = s3.Bucket(self.ingest_bucket_name)

        with tempfile.NamedTemporaryFile() as test_file:
            with open(test_file.name, 'wb') as raw_data:
                ingest_bucket.download_fileobj(VOLUMETRIC_CUBOID_KEY, raw_data)
            with open(test_file.name, 'rb') as raw_data:
                # dtype set in boss-v0.2-test.json under chunk_processor.params.info.data_type
                cuboid = self.s3_object_to_cuboid(raw_data.read(), 'uint8')
                assert expected_cuboid.shape == cuboid.shape
                assert np.array_equal(expected_cuboid, cuboid)

    def s3_object_to_cuboid(self, raw_data, data_type):
        data = blosc.decompress(raw_data)
        data_mat = np.frombuffer(data, dtype=data_type)
        return np.reshape(
            data_mat, (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y, BOSS_CUBOID_X),
            order='C')


class TestBossEngine(EngineBossTestMixin, ResponsesMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        schema_file = os.path.join(
            resource_filename("ingestclient", "schema"),
            "boss-v0.2-schema.json")
        with open(schema_file, 'r') as file_handle:
            s = json.load(file_handle)
            cls.mock_schema = {"schema": s}

        cls.config_file = os.path.join(
            resource_filename("ingestclient", "test/data"),
            "boss-v0.2-test.json")
        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # Setup AWS stuff
        cls.setup_helper = Setup()
        cls.setup_helper.mock = True
        cls.setup_helper.start_mocking()

        cls.queue_url = cls.setup_helper.create_queue("test-queue")

        cls.tile_bucket_name = "test-tile-store"
        cls.ingest_bucket_name = "test-cuboid-store"
        cls.setup_helper.create_bucket(cls.ingest_bucket_name)

        # mock api token
        cls.api_token = "aalasdklbajklsbfasdklbfkjdsb"

        # mock aws creds
        cls.aws_creds = {
            "access_key": "asdfasdf",
            "secret_key": "asdfasdfasdfadsf"
        }

    @classmethod
    def tearDownClass(cls):
        # Stop mocking
        cls.setup_helper.stop_mocking()
