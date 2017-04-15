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
        self.tile_processor = None
        self.path_processor = None
        self.backend_api_token = backend_api_token
        self.credential_create_time = None

        # Properties of ingest after creation
        self.credentials = None
        self.ingest_job_id = ingest_job_id
        self.upload_job_queue = None
        self.job_status = 0
        self.tile_bucket = None
        self.job_params = None
        self.tile_count = 0

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
        self.tile_processor = self.config.tile_processor_class
        self.tile_processor.setup(self.config.get_tile_processor_params())

        # Setup path processor
        self.path_processor = self.config.path_processor_class
        self.path_processor.setup(self.config.get_path_processor_params())

    def setup(self):
        """Method to setup the Engine by finishing configuring subclasses and validating the schema"""
        logger = logging.getLogger('ingest-client')

        msgs = self.validator.validate()

        for msg in msgs["info"]:
            logger.info(msg)

        if msgs["error"]:
            for msg in msgs["error"]:
                logger.info(msg)
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
            None


        """
        self.backend.complete(self.ingest_job_id)

    def monitor(self, workers):
        """Method to monitor the progress of the ingest job

        Returns:
            None
        """
        logger = logging.getLogger('ingest-client')
        tile_rate_samples = deque(maxlen=6)
        last_task_count = None
        start_time = time.time()
        print_time = time.time()
        avg_tile_rate = 0
        while True:
            if (datetime.datetime.now() - self.credential_create_time).total_seconds() > self.backend.credential_timeout:
                logger.warning("(pid={}) Credentials are expiring soon, attempting to renew credentials".format(os.getpid()))
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
                        log_str = "Uploading ~{:.2f} tiles/min".format(avg_tile_rate * 6)
                        log_str += " - Approx {:d} of {:d} tiles remaining".format(status["current_message_count"],
                                                                                   status["total_message_count"])
                        log_str += " - Elapsed time {:.2f} minutes".format((time.time() - start_time) / 60)
                        always_log_info(log_str)
                    else:
                        always_log_info("Waiting to ensure all upload tasks have been processed. Just a few minutes longer...")

                else:
                    always_log_info("Uploading in progress: Elapsed time {:.2f} minutes".format((time.time() - start_time) / 60))

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
        # Set up logger
        logger = logging.getLogger('ingest-client')

        # Make sure you are joined
        if not self.credentials:
            msg = "(pid={}) Cannot start ingest engine.  Credentials not successfully received from the ingest service.".format(os.getpid())
            logger.error(msg)
            raise Exception(msg)

        if self.job_status == 0:
            msg = "(pid={}) Cannot start ingest engine.  Ingest job is not ready yet.".format(os.getpid())
            logger.error(msg)
            raise Exception(msg)

        if self.job_status == 2:
            msg = "(pid={}) Ingest job already completed. Skipping ingest engine start.".format(os.getpid())
            logger.warning(msg)
            raise Exception(msg)

        # Do some work
        wait_cnt = 0
        while True:
            # Check if you need to renew credentials
            if (datetime.datetime.now() - self.credential_create_time).total_seconds() > self.backend.credential_timeout:
                logger.warning("(pid={}) Credentials are expiring soon, attempting to renew credentials".format(os.getpid()))
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
            key_parts = self.backend.decode_tile_key(msg['tile_key'])
            logger.info("(pid={}) Processing Task -  X:{} Y:{} Z:{} T:{}".format(os.getpid(),
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
                logger.info("(pid={}) Successfully wrote file: {}".format(os.getpid(), response.key))

            except Exception as e:
                logger.error("(pid={}) Upload Failed -  X:{} Y:{} Z:{} T:{} - {}".format(os.getpid(),
                                                                                         key_parts["x_index"],
                                                                                         key_parts["y_index"],
                                                                                         key_parts["z_index"],
                                                                                         key_parts["t_index"],
                                                                                         e))

