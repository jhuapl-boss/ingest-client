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


class Engine(object):
    def __init__(self, config_file=None):
        """
        A class to implement the core upload client workflow engine

        Args:
            config_file (str): Absolute path to a config file

        """
        self.config = None
        self.backend = None
        self.validator = None
        self.tile_processor = None
        self.path_processor = None

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
        self.backend = self.config.get_backend()

        # Get validator
        self.validator = self.config.get_validator()

    def check_user(self):
        pass
        # Method to check if the user is the correct one
