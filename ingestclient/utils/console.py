import sys
import json
from .log import always_log_info
import pprint


def print_estimated_job(config_file=None, configuration=None):
    """Method to print details about the job the user is about to start

    Kwd Args:
        config_file(str): Path to the config file
        configuration(Configuration): Configuration object

    Returns:
        None
    """
    if config_file is not None:
        # Load file
        with open(config_file, 'rt') as cf:
            config = json.load(cf)
    elif configuration is not None:
        config = configuration.config_data
    else:
        raise Exception('Must provide either a configuration object or an absolute path to a config file')

    # Compute number of tiles
    num_x_tiles = config["ingest_job"]["extent"]["x"][1] - config["ingest_job"]["extent"]["x"][0]
    num_y_tiles = config["ingest_job"]["extent"]["y"][1] - config["ingest_job"]["extent"]["y"][0]
    num_z_tiles = config["ingest_job"]["extent"]["z"][1] - config["ingest_job"]["extent"]["z"][0]
    num_t_tiles = config["ingest_job"]["extent"]["t"][1] - config["ingest_job"]["extent"]["t"][0]

    num_tiles = (num_x_tiles * num_y_tiles) / (config["ingest_job"]["tile_size"]["x"] * config["ingest_job"]["tile_size"]["y"])
    num_tiles = num_tiles * num_z_tiles * num_t_tiles

    # Build Message
    pp = pprint.PrettyPrinter(indent=2)
    msg = "\n\n#### INGEST JOB SUMMARY ####\n\n"
    msg += "Data will be loaded into the Boss here:\n"
    msg += "  Collection: {}\n".format(config["database"]["collection"])
    msg += "  Experiment: {}\n".format(config["database"]["experiment"])
    msg += "  Channel: {}\n\n".format(config["database"]["channel"])
    msg += "Path Processor Configuration:\n"
    msg += "  Plugin: {}\n".format(config["client"]["path_processor"]["class"])
    msg += "  Parameters: {}\n".format(pp.pformat(config["client"]["path_processor"]["params"]).replace("\n", "\n              "))
    msg += "\nTile Processor Configuration:\n"
    msg += "  Plugin: {}\n".format(config["client"]["tile_processor"]["class"])
    msg += "  Parameters: {}\n".format(pp.pformat(config["client"]["tile_processor"]["params"]).replace("\n", "\n              "))
    msg += "\nTotal Number of Image Tiles to Upload: {}".format(int(num_tiles))

    # Print/Log
    always_log_info(msg)


class WaitPrinter(object):
    """Simple class to handle a print while waiting
    """
    def __init__(self):
        self.first_print = True
        self.wait_char = "."

    def print_msg(self, msg):
        """Method to print an initial message"""
        if self.first_print:
            sys.stdout.write("{}{}{}{}".format(msg, self.wait_char, self.wait_char, self.wait_char))
            sys.stdout.flush()
            self.first_print = False
        else:
            sys.stdout.write(self.wait_char)
            sys.stdout.flush()

    def finished(self, msg=None):
        """Method to print final message"""
        if msg:
            print(msg)
        else:
            print("Complete")
