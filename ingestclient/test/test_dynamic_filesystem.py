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
from pkg_resources import resource_filename

from PIL import Image
import numpy as np

from moto import mock_s3
import boto3

from ingestclient.utils.filesystem import DynamicFilesystem, DynamicFilesystemAbsPath


class TestDynamicFilesystem(unittest.TestCase):
    mock_s3 = None

    @classmethod
    def setUpClass(cls):
        cls.config_local = {"filesystem": "local",
                            "extension": "png"}

        cls.config_s3 = {"filesystem": "s3",
                         "extension": "png",
                         "bucket": "my_bucket"}

        # Set up bucket
        cls.mock_s3 = mock_s3()
        cls.mock_s3.start()

        client = boto3.client('s3', region_name="us-east-1")
        _ = client.create_bucket(ACL='private', Bucket=cls.config_s3["bucket"])
        waiter = client.get_waiter('bucket_exists')
        waiter.wait(Bucket=cls.config_s3["bucket"])

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cls.config_s3["bucket"])

        # Put images in S3
        cls.test_imgs = [os.path.join(resource_filename("ingestclient", "test/data/example_z_stack"),
                                      "3253_my_stack_section000.png"),
                         os.path.join(resource_filename("ingestclient", "test/data/example_z_stack"),
                                      "3254_my_stack_section001.png")]

        cls.imgs = ["example_z_stack/3253_my_stack_section000.png",
                    "example_z_stack/3254_my_stack_section001.png"]

        for key, img in zip(cls.imgs, cls.test_imgs):
            # put file
            bucket.upload_file(img, key)

    @classmethod
    def tearDownClass(cls):
        cls.mock_s3.stop()

    def file_tests(self, fs, truth_filename, target_path):
        filename = fs.get_file(target_path)

        # Open handle as image file
        test_img = Image.open(filename)
        test_img = np.array(test_img, dtype="uint8")

        # Open original data
        truth_img = Image.open(truth_filename)
        truth_img = np.array(truth_img, dtype="uint8")

        # Make sure the same
        np.testing.assert_array_equal(truth_img, test_img)

    def test_local(self):
        """Test local filesystem"""
        local_base = os.path.join(resource_filename("ingestclient", "test/data"))
        fs = DynamicFilesystem("local", self.config_local)
        for truth, img in zip(self.test_imgs, self.imgs):
            self.file_tests(fs, truth, os.path.join(local_base, img))

    def test_s3(self):
        """Test the s3 filesystem"""
        fs = DynamicFilesystem("s3", self.config_s3)
        for truth, img in zip(self.test_imgs, self.imgs):
            self.file_tests(fs, truth, img)


class TestDynamicFilesystemAbsPath(unittest.TestCase):
    mock_s3 = None

    @classmethod
    def setUpClass(cls):
        cls.config_local = {"filesystem": "local",
                            "extension": "png"}

        cls.config_s3 = {"filesystem": "s3",
                         "extension": "png",
                         "bucket": "my_bucket"}

        # Set up bucket
        cls.mock_s3 = mock_s3()
        cls.mock_s3.start()

        client = boto3.client('s3', region_name="us-east-1")
        _ = client.create_bucket(ACL='private', Bucket=cls.config_s3["bucket"])
        waiter = client.get_waiter('bucket_exists')
        waiter.wait(Bucket=cls.config_s3["bucket"])

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cls.config_s3["bucket"])

        # Put images in S3
        cls.test_imgs = [os.path.join(resource_filename("ingestclient", "test/data/example_z_stack"),
                                      "3253_my_stack_section000.png"),
                         os.path.join(resource_filename("ingestclient", "test/data/example_z_stack"),
                                      "3254_my_stack_section001.png")]

        cls.imgs = ["example_z_stack/3253_my_stack_section000.png",
                    "example_z_stack/3254_my_stack_section001.png"]

        for key, img in zip(cls.imgs, cls.test_imgs):
            # put file
            bucket.upload_file(img, key)

    @classmethod
    def tearDownClass(cls):
        cls.mock_s3.stop()

    def test_local(self):
        """Test local filesystem"""
        local_base = os.path.join(resource_filename("ingestclient", "test/data"))
        fs = DynamicFilesystemAbsPath("local", self.config_local)
        for truth, img in zip(self.test_imgs, self.imgs):
            self.assertEqual(fs.get_file(os.path.join(local_base, img)), truth)

    def test_s3(self):
        """Test the s3 filesystem"""
        fs = DynamicFilesystemAbsPath("s3", self.config_s3)
        tmp_paths = []
        for img in self.imgs:
            tmp_path = fs.get_file(img)
            self.assertEqual(tmp_path, fs.fs.file_map[img])
            self.assertTrue(os.path.isfile(tmp_path))
            tmp_paths.append(tmp_path)

        # Delete the instance, so it auto-cleans up temp files.
        del fs

        # Make sure they are gone.
        for img in tmp_paths:
            self.assertFalse(os.path.isfile(img))


