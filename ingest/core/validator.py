# Copyright 2016 NeuroData (http://neurodata.io)
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
from abc import ABCMeta, abstractmethod
import six
import jsonschema
import json
import requests


@six.add_metaclass(ABCMeta)
class Validator(object):
    def __init__(self, config_file):
        """
        A class to implement the ingest job configuration file validator

        Args:
            config_file(str): Absolute path to the config file

        """
        with open(config_file, 'r') as file_handle:
            self.config = json.load(file_handle)

        self.schema = None

    def validate_schema(self):
        """
        Method to validate the JSON data against the schema

        Args:

        Returns:
            str: An error message if schema validation fails

        """
        try:
            jsonschema.validate(self.data, self.schema)
        except jsonschema.ValidationError as e:
            return e

        return None

    def validate(self):
        """
        Method to load the configuration file and select the correct validator and backend

        Args:

        Returns:
            (dict): Dictionary of "info", "question", "error" messages

        """
        schema_err_msg = self.validate_schema()
        if not schema_err_msg:
            schema_info_msg = ["Configuration file schema validation - Passed"]
            info_msg, question_msg, error_msg = self.validate_properties()
            schema_info_msg.extend(info_msg)
            return {"info": schema_info_msg,
                    "question": question_msg,
                    "error": error_msg}
        else:
            return {"info": ["Configuration file schema validation - Failed"],
                    "question": [],
                    "error": [schema_err_msg]}

    @abstractmethod
    def validate_properties(self):
        """
        Method to validate any custom properties beyond verifying that the schema was used correctly

        Args:

        Returns:
            (list(str), list(str), list(str)): a tuple of lists containing "info", "question", "error" messages

        """
        return NotImplemented

    @staticmethod
    def factory(validator_str, config_file):
        """
        Method to return a validator class based on a string
        Args:
            validator_str:

        Returns:

        """
        if validator_str == "BossValidatorV01":
            return BossValidatorV01(config_file)
        else:
            return ValueError("Unsupported validator: {}".format(validator_str))


class BossValidatorV01(Validator):
    def __init__(self, config_file):
        """
        A class to implement the ingest job configuration file validator for the Boss (docs.theBoss.io)

        Args:
            config_file(str): Absolute path to the config file

        """
        Validator.__init__(self, config_file)

    def validate_schema(self):
        """
        Method to validate the JSON data against the schema

        Args:

        Returns:
            str: An error message if schema validation fails

        """
        # Get Schema
        r = requests.get('{}://{}/ingest/schema/{}/{}/'.format(self.config['client']['backend']['protocol'],
                                                               self.config['client']['backend']['host'],
                                                               self.config['schema']['name'],
                                                               self.config['schema']['version']),
                         headers={'accept': 'application/json'})

        if r.status_code != 200:
            return "Failed to download schema. Name: {} Version: {}".format(self.config['schema']['name'],
                                                                            self.config['schema']['version'])
        else:
            self.schema = json.loads(r.json()['schema'])

        try:
            jsonschema.validate(self.config, self.schema)
        except jsonschema.ValidationError as e:
            return e

        return None

    def validate_properties(self):
        """
        Method to validate any custom properties beyond verifying that the schema was used correctly

        Args:

        Returns:
            (list(str), list(str), list(str)): a tuple of lists containing "info", "question", "error" messages

        """
        # Validate User has ndio installed and configured with token

        # Get User info and generate question to ensure user is correct

        # Verify Collection

        # Verify Experiment

        # Verify Channel

        # If channel already exists, check corners to see if data exists.  If so question user for overwrite

        # Check tile size - error if too big

        # Check backend connectivity

        return ['Validation not really implemented yet...So good to go!'], [], []
