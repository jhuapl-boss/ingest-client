import sys
import json
from .log import always_log_info
import pprint


def print_estimated_job(config_file):
    """Method to print details about the job the user is about to start

    Args:
        config_file(str): Path to the config file

    Returns:
        None
    """
    # Load file
    with open(config_file, 'rt') as cf:
        config = json.load(cf)

    # Compute number of tiles
    num_tiles = (config["ingest_job"]["extent"]["x"][1] * config["ingest_job"]["extent"]["y"][1]) / (config["ingest_job"]["tile_size"]["x"] * config["ingest_job"]["tile_size"]["y"])
    num_tiles = num_tiles * config["ingest_job"]["extent"]["z"][1] * config["ingest_job"]["extent"]["t"][1]

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


##class WorkTracker(object)