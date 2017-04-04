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
import json
from abc import ABCMeta, abstractmethod

import six
from six.moves import cPickle as pickle
import importlib
from pkg_resources import resource_filename
import os

from .validator import Validator
from .backend import Backend


@six.python_2_unicode_compatible
class ConfigPropertyObject(object):
    def __init__(self, name, data=None, help_str=None, description=None):
        """

        Args:
            name(str): Name of the property object. Will be come the Key of the object when nested in a dict
            data(dict): Dictionary of properties to initialize with
            help_str(dict(str)): Dictionary of help strings that map to keys in data
            description(str): A description of this instance
        """
        if data:
            if not isinstance(data, dict):
                raise ValueError("'data' must be a dictionary of properties to add")
            self.__dict__ = data

        if help_str:
            if not isinstance(data, dict):
                raise ValueError("'help_str' must be a dictionary of help strings which correspond to 'data' keys")
            self.__help_str = help_str

        self.__object_name = name
        self.__object_description = description

    def __iter__(self):
        for x in self.get_properties():
            yield self.__dict__[x]

    def __str__(self):
        return self.__object_name

    def add_property(self, prop_name, prop_value, help_text=None):
        """
        Method to add a property

        Args:
            prop_name(str): name of the property
            prop_value: property value. can be anything that can be a dictionary value
            help_text(str): string of help information

        Returns:
            None
        """
        self.__dict__[prop_name] = prop_value

        if help_text:
            self.__help_str[prop_name] = help_text

    def add_property_object(self, prop_obj, help_text=None):
        """
        Method to add a property

        Args:
            prop_obj(ConfigPropertyObject): The property object to add
            help_text(str): String containing help information

        Returns:
            None
        """
        self.__dict__[prop_obj.get_name()] = prop_obj

        if help_text:
            self.__help_str[prop_obj.get_name()] = help_text

    def get_name(self):
        """Method to return the object name since we are mangling the object name so it doesn't get encoded"""
        return self.__object_name

    def get_description(self):
        """Method to return the object description since we are mangling the object name so it doesn't get encoded"""
        return self.__object_description

    def get_properties(self):
        """Method to return a list of property names contained in object

        Returns:
            (list(str)): List of property names
        """
        props = []
        for prop in [x for x in self.__dict__ if "__" not in x]:
            props.append(prop)

        return props

    def get_help_str(self, property_name):
        """Method to return the help string for a specific property

        Args:
            property_name(str): The name of the property

        Returns:
            (str): Help string
        """
        return self.__help_str[property_name]

    def explain(self):
        """Method to print help information for this configuration object

        Returns:
            None
        """
        print("\n\n### {} ###".format(self.__object_name))
        if self.__object_description:
            print(self.__object_description)
            print("")
        for prop in self.get_properties():
            print(" - {}: {}".format(prop, self.get_help_str(prop)))
        print("\n")

    def to_dict(self):
        """Method to recursively encode as a dictionary."""
        output = {}
        for prop in [x for x in self.__dict__ if "__" not in x]:
            if isinstance(self.__dict__[prop], ConfigPropertyObject):
                output.update(self.__dict__[prop].to_dict())
            else:
                # An actual property
                output[prop] = self.__dict__[prop]

        return {self.__object_name: output}


@six.add_metaclass(ABCMeta)
class ConfigurationGenerator(object):
    def __init__(self):
        self.config = ConfigPropertyObject("ROOT")
        self.name = "Base Config"
        self.description = ""

        # Setup the config
        self.setup()

    def load(self, file_path):
        """Method to pre-populate the class

        Args:
            file_path(str): Absolute path to the saved instance

        Returns:
            None
        """
        with open(file_path, 'rb') as file_handle:
            self.__dict__ = pickle.load(file_handle)

    def save(self, file_path):
        """

        Args:
            file_path(str): Absolute path to the file for saving a copy of the instance

        Returns:
            None
        """
        with open(file_path, 'wb') as file_handle:
            if six.PY3:
                pickle.dump(self.__dict__, file_handle, 2, fix_imports=True)
            else:
                pickle.dump(self.__dict__, file_handle, 2)

    @abstractmethod
    def setup(self):
        """Method to setup in instance by populating the correct data/property objects"""
        raise NotImplemented

    def to_json(self, file_path):
        """Method to serialize to json

        Args:
            file_path(str): Absolute path to the file for saving the instance as JSON

        Returns:
            None
        """
        config_dict = {}
        for obj in self.config:
            config_dict.update(obj.to_dict())

        with open(file_path, 'wt') as file_handle:
            json.dump(config_dict, file_handle)

    def explain(self):
        """Method to print help information for creating configuration

        Returns:
            None
        """
        print("\n\n### {} ###".format(self.name))
        if self.description:
            print(self.description)
            print("")
        for prop in self.config:
            print(" - {}: {}".format(prop.get_name(), prop.get_description()))
        print("\n")

    def add_property_object(self, prop_obj, help_text=None):
        """
        Method to add a property

        Args:
            prop_obj(ConfigPropertyObject): The property object to add
            help_text(str): String containing help information

        Returns:
            None
        """
        if not isinstance(prop_obj, ConfigPropertyObject):
            raise ValueError("Must add ConfigPropertyObject instance type")

        self.config.add_property_object(prop_obj, help_text=help_text)


class BossConfigurationGenerator(ConfigurationGenerator):
    def __init__(self):
        ConfigurationGenerator.__init__(self)

        self.name = "Boss Ingest v0.1"
        self.description = "Configuration Generator for the Boss ingest service v0.1"

    def setup(self):
        """Method to setup instance by populating the correct data/property objects"""
        # Schema section
        schema = ConfigPropertyObject("schema",
                                      {"version": "0.1",
                                       "name": "boss",
                                       "validator": "BossValidatorV01"},
                                      {"version": "Ingest service version number",
                                       "name": "Ingest service type",
                                       "validator": "The validator class to use to validate the schema"},
                                      "Schema properties for validation"
                                      )

        # Client Section
        client_backend = ConfigPropertyObject("backend",
                                              {"name": "boss",
                                               "class": "BossBackend",
                                               "host": "api.theboss.io",
                                               "protocol": "https"},
                                              {"name": "the backend name",
                                               "class": "the class to load for handling this backend",
                                               "host": "the domain name for the ingest server",
                                               "protocol": "https or http"},
                                              "Properties for the backend service"
                                              )
        client_tile_processor = ConfigPropertyObject("tile_processor",
                                                     {"class": "",
                                                      "params": None},
                                                     {"class": "The name of the class to load for tile processing",
                                                      "params": "Custom properties in a dictionary"},
                                                     "Properties for the custom tile processor class"
                                                     )
        client_file_processor = ConfigPropertyObject("file_processor",
                                                     {"class": "",
                                                      "params": None},
                                                     {"class": "The name of the class to load for file processing",
                                                      "params": "Custom properties in a dictionary"},
                                                     "Properties for the custom file processor class"
                                                     )

        client = ConfigPropertyObject("client",
                                      {"backend": client_backend,
                                       "tile_processor": client_tile_processor,
                                       "file_processor": client_file_processor},
                                      {"backend": "Properties for the backend service",
                                       "tile_processor": "Ingest service type",
                                       "file_processor": "The validator class to use to validate the schema"},
                                      "Ingest client properties"
                                      )

        database = ConfigPropertyObject("database",
                                        {"collection": "",
                                         "experiment": "",
                                         "channel": ""},
                                        {"collection": "The collection name containing the experiment and channel where data will be written",
                                         "experiment": "The experiment name containing the channel where data will be written",
                                         "channel": "The channel name where data will be written"},
                                        "Properties describing where in the database data should be written"
                                        )

        # Ingest Job Section
        ingest_job_extent = ConfigPropertyObject("extent",
                                                 {"x": [0, 1],
                                                  "y": [0, 1],
                                                  "z": [0, 1],
                                                  "t": [0, 1]},
                                                 {"x": "The spatial extent (in pixels) in the x-dimension. Python convention - start inclusive, stop exclusive",
                                                  "y": "The spatial extent (in pixels) in the y-dimension. Python convention - start inclusive, stop exclusive",
                                                  "z": "The spatial extent (in pixels) in the z-dimension. Python convention - start inclusive, stop exclusive",
                                                  "t": "The spatial extent (in pixels) in the t-dimension. Python convention - start inclusive, stop exclusive"},
                                                 "The spatial extent of the data to be uploaded.  This does not have to be the entire dataset."
                                                 )
        ingest_job_tile_size = ConfigPropertyObject("tile_size",
                                                 {"x": 4096,
                                                  "y": 4096,
                                                  "z": 1,
                                                  "t": 0},
                                                 {"x": "The size (in pixels) in the x-dimension of an individual image file",
                                                  "y": "The size (in pixels) in the y-dimension of an individual image file",
                                                  "z": "The size (in pixels) in the z-dimension of an individual image file",
                                                  "t": "The number of time samples in a single file"},
                                                 "The dimensions of the individual image data files that will be uploaded to the Boss"
                                                 )
        ingest_job = ConfigPropertyObject("ingest_job",
                                          {"resolution": 0,
                                           "extent": ingest_job_extent,
                                           "tile_size": ingest_job_tile_size},
                                          {"extent": "The spatial extent of the data to be uploaded.  This does not have to be the entire dataset.",
                                           "resolution": "The resolution level to ingest data. Default is 0 which represents native resolution",
                                           "tile_size": "The dimensions of the individual image data files that will be uploaded to the Boss"},
                                          "The properties defining what part of the dataset will be ingested"
                                          )

        self.add_property_object(schema)
        self.add_property_object(client)
        self.add_property_object(database)
        self.add_property_object(ingest_job)


class ConfigFileError(Exception):
    """Custom error to catch config file errors upstream in the client"""
    pass


class Configuration(object):
    def __init__(self, config_data=None):
        """
        A class to store configuration information and parameters for an ingest job

        Args:
            config_data(dict): The configuration dictionary
        """
        self.config_data = None
        self.schema = None
        self.tile_processor_class = None
        self.path_processor_class = None

        # If a configuration file was provided, load it now
        if config_data:
            self.load(config_data)

    def load(self, config_data):
        """
        Method to load the configuration file, the configuration schema, and select the correct validator and backend

        Args:
            config_data(dict): The configuration dictionary

        Returns:
            None

        """
        self.config_data = config_data

        # Load the schema file based on the config that was provided
        try:
            schema_name = self.config_data['schema']['name']
        except KeyError as err:
            raise ConfigFileError("The specified schema was not found: {}. Try to update your ingest client library or double check your ingest job configuration file".format(self.config_data['schema']['name']))
        with open(os.path.join(resource_filename("ingestclient", "schema"), "{}.json".format(schema_name)), 'rt') as schema_file:
            self.schema = json.load(schema_file)

    def load_plugins(self):
        """Method to load the plugins

        Returns:
            None
        """
        # Create plugin instances
        package, class_name = self.config_data["client"]["tile_processor"]["class"].rsplit('.', 1)
        tile_module = importlib.import_module(package)
        tile_class = getattr(tile_module, class_name)
        self.tile_processor_class = tile_class()

        package, class_name = self.config_data["client"]["path_processor"]["class"].rsplit('.', 1)
        path_module = importlib.import_module(package)
        path_class = getattr(path_module, class_name)
        self.path_processor_class = path_class()

    def get_tile_processor_params(self):
        """Method to get the tile processor parameter dictionary

        Returns:
            (dict): Dictionary of params from the config file
        """
        params = self.config_data["client"]["tile_processor"]["params"]
        params["ingest_job"] = self.config_data["ingest_job"]
        return params

    def get_path_processor_params(self):
        """Method to get the path processor parameter dictionary

        Returns:
            (dict): Dictionary of params from the config file
        """
        params = self.config_data["client"]["path_processor"]["params"]
        params["ingest_job"] = self.config_data["ingest_job"]
        return params

    def get_validator(self):
        """
        Method to get a validator instance based on the configuration

        Returns:
            (ingestclient.core.validator.Validator): Validator instance

        """
        if not self.config_data:
            raise ValueError("No configuration file loaded.")

        # Setup Validator while sanitizing input
        if any(x in self.config_data["schema"]["validator"] for x in [";", ".", "import"]):
            raise ValueError("Schema Validator Class contains dangerous syntax. Please only list the Class Name.")
        else:
            return Validator.factory(self.config_data["schema"]["validator"], self.config_data)

    def get_backend(self, backend_api_token=None):
        """
        Method to get a backend instance based on the configuration

        Returns:
            (ingestclient.core.backend.Backend): Backend instance

        """
        if not self.config_data:
            raise ValueError("No configuration file loaded.")

        # Setup Backend while sanitizing input
        if any(x in self.config_data["client"]["backend"]["class"] for x in [";", ".", "import"]):
            raise ValueError("Backend Class contains dangerous syntax. Please only list the Class Name.")
        else:
            backend = Backend.factory(self.config_data["client"]["backend"]["class"],
                                      self.config_data)

        # Setup the backend
        backend.setup(backend_api_token)

        return backend

    def to_json(self):
        """
        Method to return a JSON string containing the configuration

        Returns:
            (str): JSON encoded config data
        """
        return json.dumps(self.config_data)

