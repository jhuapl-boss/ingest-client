# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from six.moves import input
import logging
import datetime
import json
import time
from ..utils.log import always_log_info
import os
from math import floor
import random
from .config import Configuration, ConfigFileError
from collections import deque
import blosc
import numpy as np
from .consts import BOSS_CUBOID_X, BOSS_CUBOID_Y, BOSS_CUBOID_Z 
from ..plugins.chunk import XYZ_ORDER, ZYX_ORDER, XYZT_ORDER, TZYX_ORDER


class Engine(object):
    def __init__(self, config_file=None, backend_api_token=None, ingest_job_id=None, configuration=None):
        """
        A class to implement the core upload client workflow engine

        Args:
            config_file (str): Absolute path to a config file
            ingest_job_id (int): ID of the ingest job you want to work on
            backend_api_token (str): The authorization token for the Backend if used
            configuration(ingestclient.core.config.Configuration): A pre-loaded configuration instance
        """
        self.config = None
        self.msg_wait_iterations = 20  # Each iteration waits for 10 seconds for incoming messages
        self.backend = None
        self.validator = None
        self.chunk_processor = None
        self.tile_processor = None
        self.path_processor = None
        self.backend_api_token = backend_api_token
        self.credential_create_time = None
        self.logger = logging.getLogger('ingest-client')

        # Number of cuboids in a chunk for volumetric ingest.  This will be
        # updated from the ingest config.
        self.num_x_cuboids = 1
        self.num_y_cuboids = 1
        self.num_z_cuboids = 1

        # Properties of ingest after creation
        self.credentials = None
        self.ingest_job_id = ingest_job_id
        self.upload_job_queue = None
        self.job_status = 0
        # When running a volumetric ingest, tile_bucket points to a bucket for
        # cuboids.
        self.tile_bucket = None
        self.job_params = None
        self.tile_count = 0
        self.access_denied = False
        self.access_denied_count = 0
        self.invalid_access_key = False
        self.invalid_access_key_count = 0

        if configuration:
            self.configure(configuration)
        elif config_file:
            self.configure_from_file(config_file)

    def configure_from_file(self, config_file):
        """
        Method to load a configuration file and setup the workflow engine
        Args:
            config_file (str): Absolute path to a config file

        Returns:
            None
        """
        try:
            with open(config_file, 'r') as file_handle:
                config_data = json.load(file_handle)
        except ValueError as _:
            # Bad json file
            raise ConfigFileError(
                "Malformed JSON in Ingest Configuration File.  Please double check contents and try again")
        except IOError as _:
            # File not found - python2/3 are different for missing files so us OSError
            raise ConfigFileError(
                "Ingest Configuration File not found.  Double check the provided path: {}".format(config_file))
        except OSError as _:
            # File not found - python2/3 are different for missing files so us OSError
            raise ConfigFileError(
                "Ingest Configuration File not found.  Double check the provided path: {}".format(config_file))

        self.configure(Configuration(config_data))

    def configure(self, configuration):
        """
        Method to apply a configuration and setup the workflow engine
        Args:
            configuration (Configuration)

        Returns:
            None
        """

        # Load Config file and validate
        self.config = configuration
        self.config.load_plugins()

        # Get backend
        self.backend = self.config.get_backend(self.backend_api_token)

        # Get validator and set config
        self.validator = self.config.get_validator()
        self.validator.schema = self.config.schema

        # Setup tile processor
        if ("ingest_type" not in self.config.config_data["ingest_job"] or
                self.config.config_data["ingest_job"]["ingest_type"] == "tile"):
            self.tile_processor = self.config.tile_processor_class
            self.tile_processor.setup(self.config.get_tile_processor_params())

        # Setup chunk processor
        if ("ingest_type" in self.config.config_data["ingest_job"] and
                self.config.config_data["ingest_job"]["ingest_type"] == "volumetric"):
            self.chunk_processor = self.config.chunk_processor_class
            self.chunk_processor.setup(self.config.get_chunk_processor_params())
            self.num_x_cuboids = self.config.config_data["ingest_job"]["chunk_size"]["x"] // BOSS_CUBOID_X
            self.num_y_cuboids = self.config.config_data["ingest_job"]["chunk_size"]["y"] // BOSS_CUBOID_Y
            self.num_z_cuboids = self.config.config_data["ingest_job"]["chunk_size"]["z"] // BOSS_CUBOID_Z

        # Setup path processor
        self.path_processor = self.config.path_processor_class
        self.path_processor.setup(self.config.get_path_processor_params())

    def setup(self):
        """Method to setup the Engine by finishing configuring subclasses and validating the schema"""
        msgs = self.validator.validate()

        for msg in msgs["info"]:
            self.logger.info(msg)

        if msgs["error"]:
            for msg in msgs["error"]:
                self.logger.info(msg)
            raise Exception("Validation Failed: {}".format(" - ".join(msgs["error"])))

        return msgs["question"]

    def create_job(self):
        """
        Method to create an ingest job

        Args:

        Returns:
            None


        """
        self.ingest_job_id = self.backend.create(self.config.config_data)

        always_log_info("CREATED INGEST JOB: {}".format(self.ingest_job_id))

    def join(self):
        """
        Method to join an ingest job upload

        Job Status: {0: Preparing, 1: Uploading, 2: Complete}

        Args:


        Returns:
            None


        """
        self.job_status, self.credentials, self.upload_job_queue, self.tile_bucket, self.job_params, self.tile_count = self.backend.join(self.ingest_job_id)

        # Set cred time
        self.credential_create_time = datetime.datetime.now()
        always_log_info("(pid={}) JOINED INGEST JOB: {}".format(os.getpid(), self.ingest_job_id))

    def cancel(self):
        """
        Method to cancel an ingest job

        Args:

        Returns:
            None


        """
        self.backend.cancel(self.ingest_job_id)

    def complete(self):
        """
        Method to complete an ingest job

        Args:

        Returns:
            (bool): True if successfully completed job.


        """
        return self.backend.complete(self.ingest_job_id)

    def _get_units(self):
        """Get appropriate units (tiles or chunks) for reporting status
        
        Returns:
            (str): 'tiles' or 'chunks'

        Raises:
            (ValueError): if ingest_type is unknown
        """
        if ("ingest_type" not in self.config.config_data["ingest_job"] or
                self.config.config_data["ingest_job"]["ingest_type"] == "tile"):
            return "tile"
        elif self.config.config_data["ingest_job"]["ingest_type"] == "volumetric":
            return "chunks"

        raise ValueError("Invalid ingest_type: {}".format(
            self.config.config_data["ingest_job"]["ingest_type"]))

    def monitor(self, workers):
        """Method to monitor the progress of the ingest job

        Returns:
            None
        """
        tile_rate_samples = deque(maxlen=6)
        last_task_count = None
        start_time = time.time()
        print_time = time.time()
        avg_tile_rate = 0
        units = self._get_units()

        while True:
            total_seconds = (datetime.datetime.now() - self.credential_create_time).total_seconds()
            if total_seconds > self.backend.credential_timeout:
                self.logger.warning("(pid={}) Credentials are expiring soon, attempting to renew credentials".format(
                    os.getpid()))
                self.join()
                always_log_info("(pid={}) Credentials refreshed successfully".format(os.getpid()))

            status = self.backend.get_job_status(self.ingest_job_id)
            if status:
                if last_task_count is None:
                    last_task_count = status["current_message_count"]
                    continue

                tile_rate_samples.append(last_task_count - status["current_message_count"])
                last_task_count = status["current_message_count"]

                avg_tile_rate = sum(tile_rate_samples) / float(len(tile_rate_samples))

            if (time.time() - print_time) > 30:
                print_time = time.time()
                # Print an update every 30 seconds
                if status:
                    if status["current_message_count"] != 0:
                        log_str = "Uploading ~{:.2f} {}/min".format(avg_tile_rate * 6, units)
                        log_str += " - Approx {:d} of {:d} {} remaining".format(status["current_message_count"],
                                                                                status["total_message_count"],
                                                                                units)
                        log_str += " - Elapsed time {:.2f} minutes".format((time.time() - start_time) / 60)
                        always_log_info(log_str)
                    else:
                        log_str = "Waiting to ensure all upload tasks have been processed. Just a few minutes longer..."
                        always_log_info(log_str)

                else:
                    log_str = "Uploading in progress: Elapsed time {:.2f} minutes"
                    log_str = log_str.format((time.time() - start_time) / 60)
                    always_log_info(log_str)

            # Wait to loop
            time.sleep(10)

            # Check to see if worker processes have all ended
            alive_cnt = 0
            for worker in workers:
                if worker[0].is_alive():
                    alive_cnt += 1

            if alive_cnt == 0:
                # if no processes are alive you are done (or something broke)! Bail.
                break

    def run(self):
        """Method to run the upload loop

        Returns:

        """
        # Make sure you are joined
        if not self.credentials:
            msg = "(pid={}) Cannot start ingest engine.  Credentials not successfully received from the ingest service.".format(os.getpid())
            self.logger.error(msg)
            raise Exception(msg)

        if self.job_status == 0:
            msg = "(pid={}) Cannot start ingest engine.  Ingest job is not ready yet.".format(os.getpid())
            self.logger.error(msg)
            raise Exception(msg)

        if self.job_status == 2:
            msg = "(pid={}) Ingest job already completed. Skipping ingest engine start.".format(os.getpid())
            self.logger.warning(msg)
            raise Exception(msg)

        # Do some work
        self.access_denied = False
        self.access_denied_count = 0
        self.invalid_access_key = False
        self.invalid_access_key_count = 0

        wait_cnt = 0
        while True:
            if self.access_denied:
                self.access_denied = False
                self.credential_create_time = datetime.datetime.min
            if self.invalid_access_key:
                self.invalid_access_key = False
                if self.invalid_access_key_count % 5 == 4:
                    # We check for a few times before setting the credentials to be renewed
                    # because it is possible these are new credentials that have not become valid yet.
                    self.credential_create_time = datetime.datetime.min
            # Check if you need to renew credentials
            total_seconds = (datetime.datetime.now() - self.credential_create_time).total_seconds()
            if total_seconds > self.backend.credential_timeout:
                self.logger.warning("(pid={}) Credentials are expiring soon, attempting to renew credentials".format(
                    os.getpid()))
                self.join()
                always_log_info("(pid={}) Credentials refreshed successfully".format(os.getpid()))

            # Get a task
            message_id, receipt_handle, msg = self.backend.get_task()

            if not msg:
                time.sleep(10)
                wait_cnt += 1
                if wait_cnt < self.msg_wait_iterations:
                    continue
                else:
                    break

            wait_cnt = 0

            if ("ingest_type" not in self.config.config_data["ingest_job"] or
                    self.config.config_data["ingest_job"]["ingest_type"] == "tile"):
                if not self.upload_tile(msg, message_id, receipt_handle):
                    break
            elif self.config.config_data["ingest_job"]["ingest_type"] == "volumetric":
                if not self.upload_chunk(msg, message_id, receipt_handle):
                    break
            else:
                self.logger.error("Invalid ingest_type specified: {}".format(self.config.config_data["ingest_job"]["ingest_type"]))
                return

    def upload_tile(self, msg, message_id, receipt_handle):
        """Upload a single tile

        Args:
            msg (dict): Message contents from upload queue
            message_id (str):
            receipt_handle (str): Necessary to remove the message from the upload queue

        Returns:
            (bool): False if upload should be aborted.
        """
        key_parts = self.backend.decode_tile_key(msg['tile_key'])
        self.logger.info("(pid={}) Processing Task -  X:{} Y:{} Z:{} T:{}".format(os.getpid(),
                                                                             key_parts["x_index"],
                                                                             key_parts["y_index"],
                                                                             key_parts["z_index"],
                                                                             key_parts["t_index"]))

        # Call path processor
        filename = self.path_processor.process(key_parts["x_index"],
                                               key_parts["y_index"],
                                               key_parts["z_index"],
                                               key_parts["t_index"])

        # Call tile processor
        handle = self.tile_processor.process(filename,
                                             key_parts["x_index"],
                                             key_parts["y_index"],
                                             key_parts["z_index"],
                                             key_parts["t_index"])

        try:
            metadata = {'chunk_key': msg['chunk_key'],
                        'ingest_job': self.ingest_job_id,
                        'parameters': self.job_params,
                        'x_size': self.config.config_data['ingest_job']["tile_size"]["x"],
                        'y_size': self.config.config_data['ingest_job']["tile_size"]["y"],
                        }
            handle.seek(0)
            response = self.backend.bucket.put_object(ACL='private',
                                                      Body=handle,
                                                      Key=msg['tile_key'],
                                                      Metadata={
                                                          'message_id': message_id,
                                                          'receipt_handle': receipt_handle,
                                                          'metadata': json.dumps(metadata, separators=(',', ':'))
                                                      },
                                                      StorageClass='STANDARD')
            self.logger.info("(pid={}) Successfully wrote file: {}".format(os.getpid(), response.key))

        except Exception as e:
            self.logger.error("(pid={}) Upload Tile Failed -  X:{} Y:{} Z:{} T:{} - {}".format(os.getpid(),
                                                                                     key_parts["x_index"],
                                                                                     key_parts["y_index"],
                                                                                     key_parts["z_index"],
                                                                                     key_parts["t_index"],
                                                                                     e))
            if str(e).startswith("An error occurred (AccessDenied) when calling the PutObject operation"):
                self.access_denied = True
                self.access_denied_count += 1
                if self.access_denied_count >= 20:
                    self.logger.error("(pid={}) failed 20 times with same error, breaking out of loop: {} ".format(
                        os.getpid(), e))
                    return False
            elif str(e).startswith("An error occurred (InvalidAccessKeyId) when calling the PutObject operation"):
                time.sleep(5)
                self.invalid_access_key = True
                self.invalid_access_key_count += 1
                if self.invalid_access_key_count >= 20:
                    self.logger.error("(pid={}) failed 20 times with same error, breaking out of loop: {} ".format(
                        os.getpid(), e))
                    return False

        return True

    def upload_chunk(self, msg, message_id, receipt_handle):
        """Upload a single chunk as cuboids.

        Args:
            msg (dict): Message contents from upload queue
            message_id (str):
            receipt_handle (str): Necessary to remove the message from the upload queue

        Returns:
            (bool): False if upload should be aborted.
        """
        key_parts = self.backend.decode_chunk_key(msg['chunk_key'])
        self.logger.info("(pid={}) Processing Chunk -  X:{} Y:{} Z:{} ".format(os.getpid(),
                                                                         key_parts["x_index"],
                                                                         key_parts["y_index"],
                                                                         key_parts["z_index"]))

        # Call path processor
        filename = self.path_processor.process(key_parts["x_index"],
                                               key_parts["y_index"],
                                               key_parts["z_index"],
                                               key_parts["t_index"])

        # Call chunk processor
        chunk, array_order = self.chunk_processor.process(filename,
                                                          key_parts["x_index"],
                                                          key_parts["y_index"],
                                                          key_parts["z_index"])

        # Make sure chunk is a C order NdArray.
        if not chunk.flags['C_CONTIGUOUS']:
            # Data is in a Fortran-style contiguous segment, NOT C-style so
            # need to convert.
            c_order_chunk = np.ascontiguousarray(chunk)
        else:
            c_order_chunk = chunk

        # Allow for potential garbage collection if chunk was Fortan order.
        del chunk

        for cuboid_data in msg['cuboids']:
            x = cuboid_data['x']
            y = cuboid_data['y']
            z = cuboid_data['z']
            if not self.upload_cuboid(
                c_order_chunk, x, y, z, cuboid_data['key'], msg['chunk_key'], array_order
            ):
                return False

        # Successfully uploaded all cuboids - delete message from upload queue.
        self.backend.delete_task(message_id, receipt_handle)

        return True

    def upload_cuboid(self, chunk, x, y, z, key, chunk_key, array_order):
        """
        Upload a single Boss cuboid to the ingest bucket.

        Args:
            chunk (NdArray): The chunk of cuboids in C order.
            x (int): The starting index in x of the cuboid.
            y (int): The starting index in y of the cuboid.
            z (int): The starting index in z of the cuboid.
            key (str): S3 object key for storing this cuboid in the bucket.
            chunk_key (str): Chunk key will be stored in the S3 object's metadata.
            array_order (int): Order of elements in array (XYZ_ORDER, TZYX_ORDER, etc).

        Returns:
            (bool): False if upload should be aborted.
        """
        try:
            metadata = {
                'ingest_job': self.ingest_job_id,
                'chunk_key': chunk_key,
                'parameters': self.job_params
            }

            if array_order == XYZ_ORDER or array_order == XYZT_ORDER:
                raw_sub_chunk = chunk[x:x+BOSS_CUBOID_X, y:y+BOSS_CUBOID_Y, z:z+BOSS_CUBOID_Z]
                # Fix ordering so either Z or T first.
                ordered_array = np.transpose(raw_sub_chunk)
            else:
                ordered_array = chunk[z:z+BOSS_CUBOID_Z, y:y+BOSS_CUBOID_Y, x:x+BOSS_CUBOID_X]

            # Add time dimension to array.
            if array_order == XYZ_ORDER or array_order == ZYX_ORDER:
                array_4d = np.expand_dims(ordered_array, 0)
            else:
                array_4d = ordered_array

            # Grow a partial cuboid to full size.
            if array_4d.shape != (1, BOSS_CUBOID_Z, BOSS_CUBOID_Y, BOSS_CUBOID_X):
                z_diff = BOSS_CUBOID_Z - array_4d.shape[1]
                y_diff = BOSS_CUBOID_Y - array_4d.shape[2]
                x_diff = BOSS_CUBOID_X - array_4d.shape[3]
                array_4d = np.pad(
                    array_4d, ((0, 0), (0, z_diff), (0, y_diff), (0, x_diff)),
                    'constant', constant_values=0)

            # Ensure array is in C order.
            if not array_4d.flags['C_CONTIGUOUS']:
                data = np.ascontiguousarray(array_4d)
            else:
                data = array_4d

            # Assume chunk has data type that matches the Boss channel.
            compressed_data = blosc.compress(data, typesize=(data.dtype).itemsize * 8)

            response = self.backend.volumetric_bucket.put_object(ACL='private',
                                                      Body=compressed_data,
                                                      Key=key,
                                                      Metadata={
                                                          'metadata': json.dumps(metadata, separators=(',', ':'))
                                                      },
                                                      StorageClass='STANDARD')
            self.logger.info("(pid={}) Successfully uploaded cuboid: {}".format(os.getpid(), response.key))

        except Exception as e:
            self.logger.error("(pid={}) Upload Cuboid Failed -  X:{} Y:{} Z:{} - {}".format(os.getpid(), x, y, z, e))
            if str(e).startswith("An error occurred (AccessDenied) when calling the PutObject operation"):
                self.access_denied = True
                self.access_denied_count += 1
                if self.access_denied_count >= 20:
                    self.logger.error("(pid={}) failed 20 times with same error, breaking out of loop: {} ".format(
                        os.getpid(), e))
                    return False
            elif str(e).startswith("An error occurred (InvalidAccessKeyId) when calling the PutObject operation"):
                time.sleep(5)
                self.invalid_access_key = True
                self.invalid_access_key_count += 1
                if self.invalid_access_key_count >= 20:
                    self.logger.error("(pid={}) failed 20 times with same error, breaking out of loop: {} ".format(
                        os.getpid(), e))
                    return False

        return True
