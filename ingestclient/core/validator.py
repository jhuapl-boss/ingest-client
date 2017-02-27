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


@six.add_metaclass(ABCMeta)
class Validator(object):
    def __init__(self, config_data):
        """
        A class to implement the ingest job configuration file validator

        Args:
            config_data(dict): Dictionary of configuration

        """
        self.config = config_data

        # Schema to be populated by backend
        self.schema = None

    def validate_schema(self):
        """
        Method to validate the JSON data against the schema

        Args:

        Returns:
            str: An error message if schema validation fails

        """
        if not self.schema:
            raise ValueError("Schema has not been populated yet. Cannot validate.")

        try:
            jsonschema.validate(self.config, self.schema)
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
                    "error": [schema_err_msg.message]}

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
    def factory(validator_str, config_data):
        """
        Method to return a validator class based on a string
        Args:
            validator_str(str): Class name for the validator to use
            config_data(dict): Dictionary containing configuration data

        Returns:

        """
        if validator_str == "BossValidatorV01":
            return BossValidatorV01(config_data)
        else:
            return ValueError("Unsupported validator: {}".format(validator_str))


class BossValidatorV01(Validator):
    def __init__(self, config_data):
        """
        A class to implement the ingest job configuration file validator for the Boss (docs.theBoss.io)

        Args:
            config_data(dict): Configuration dictionary

        """
        Validator.__init__(self, config_data)

    def validate_properties(self):
        """
        Method to validate any custom properties beyond verifying that the schema was used correctly

        Args:

        Returns:
            (list(str), list(str), list(str)): a tuple of lists containing "info", "question", "error" messages

        """
        # TODO: Add Boss specific validation
        # Verify Collection

        # Verify Experiment

        # Verify Channel

        # If channel already exists, check corners to see if data exists.  If so question user for overwrite

        # Check tile size - error if too big

        # Check backend connectivity

        return ['Parameter Validation Passed'], [], []
