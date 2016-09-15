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
from ingest.core.config import Configuration
import boto3
from six.moves import input
import logging
import datetime


class Engine(object):
    def __init__(self, config_file=None, backend_api_token=None, ingest_job_id=None):
        """
        A class to implement the core upload client workflow engine

        Args:
            config_file (str): Absolute path to a config file
            ingest_job_id (int): ID of the ingest job you want to work on
            backend_api_token (str): The authorization token for the Backend if used

        """
        self.config = None
        self.backend = None
        self.validator = None
        self.tile_processor = None
        self.path_processor = None
        self.backend_api_token = backend_api_token

        # Properties of ingest after creation
        self.credentials = None
        self.ingest_job_id = ingest_job_id
        self.upload_job_queue = None
        self.job_status = 0

        self.tile_bucket = None

        if config_file:
            self.load_configuration(config_file)

    def load_configuration(self, config_file):
        """
        Method to load a configuration file and setup the workflow engine
        Args:
            config_file (str): Absolute path to a config file

        Returns:
            None
        """
        # Load Config file and validate
        self.config = Configuration(config_file)

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

    def setup(self, log_file=None):
        """Method to setup the Engine by finishing configuring subclasses and validating the schema"""
        if not log_file:
            log_file = 'ingest_log{}.log'.format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename=log_file,
                            filemode='a')
        logging.getLogger('ingest-client').addHandler(logging.StreamHandler())
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
        self.ingest_job_id = self.backend.create(self.config.to_json())

    def join(self):
        """
        Method to join an ingest job upload

        Job Status: {0: Preparing, 1: Uploading, 2: Complete}

        Args:


        Returns:
            None


        """
        self.job_status, self.credentials, self.upload_job_queue, tile_bucket = self.backend.join(self.ingest_job_id)

        # Setup bucket
        # TODO: Possibly replace if ndingest lib is used as a dependency
        s3 = boto3.resource('s3')
        self.tile_bucket = s3.Bucket(tile_bucket)

    def cancel(self):
        """
        Method to cancel an ingest job

        Args:

        Returns:
            None


        """
        self.backend.cancel(self.ingest_job_id)

    def run(self):
        """Method to run the upload loop

        Returns:

        """
        # Set up logger
        logger = logging.getLogger('ingest-client')

        # Make sure you are joined
        if not self.credentials:
            msg = "Cannot start ingest engine.  You must first join an ingest job!"
            logger.error(msg)
            raise Exception(msg)

        if self.job_status == 0:
            msg = "Cannot start ingest engine.  Ingest job is not ready yet"
            logger.error(msg)
            raise Exception(msg)

        if self.job_status == 2:
            msg = "Ingest job already completed. Skipping ingest engine start."
            logger.info(msg)
            raise Exception(msg)

        # Do some work
        while True:
            try:
                # Get a task
                message_id, receipt_handle, msg = self.backend.get_task()

                if not msg:
                    break

                logger.info("Processing Task -  X:{} Y:{} Z:{} T:{}".format(msg["x_tile"],
                                                                            msg["y_tile"],
                                                                            msg["z_tile"],
                                                                            msg["time_sample"]))

                # Call path processor
                filename = self.path_processor.process(msg["x_tile"],
                                                       msg["y_tile"],
                                                       msg["z_tile"],
                                                       msg["time_sample"])

                # Call tile processor
                handle = self.tile_processor.process(filename, msg["x_tile"],
                                                               msg["y_tile"],
                                                               msg["z_tile"],
                                                               msg["time_sample"])

                # Upload tile
                project_info = [msg["collection"], msg["experiment"], msg["channel"]]
                object_key = self.backend.encode_object_key(project_info,
                                                            msg["resolution"],
                                                            msg["x_tile"],
                                                            msg["y_tile"],
                                                            msg["z_tile"],
                                                            msg["time_sample"])
                try:
                    response = self.tile_bucket.put_object(ACL='private',
                                                           Body=handle,
                                                           Key=object_key,
                                                           Metadata={
                                                               'message_id': message_id,
                                                               'receipt_handle': receipt_handle,
                                                               "queue_url": self.backend.queue.url
                                                           },
                                                           StorageClass='STANDARD')
                    logger.info("Successfully wrote file: {}".format(response.key))

                except Exception as e:
                    logger.error("Upload Failed -  X:{} Y:{} Z:{} T:{} - {}".format(msg["x_tile"],
                                                                                    msg["y_tile"],
                                                                                    msg["z_tile"],
                                                                                    msg["time_sample"],
                                                                                    e))

            except KeyboardInterrupt:
                # Make sure they want to stop this client
                quit_run = False
                while True:
                    quit_uploading = input("Are you sure you want to quit uploading? (y/n)")
                    if quit_uploading.lower() == "y":
                        quit_run = True
                        break
                    elif quit_uploading.lower() == "n":
                        print("Continuing...")
                        break
                    else:
                        print("Enter 'y' or 'n' for 'yes' or 'no'")

                if quit_run:
                    print("Stopping upload engine.")
                    break

        logger.info("No more tasks remaining.")



