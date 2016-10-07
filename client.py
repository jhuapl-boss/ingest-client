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
from ingest.core.engine import Engine
from ingest.core.config import ConfigFileError
from six.moves import input
import argparse
import sys


def get_confirmation(prompt):
    """Method to confirm decisions

    Args:
        prompt(str): Question to ask the user

    Returns:
        (bool): True indicating yes, False indicating no
    """
    decision = False
    while True:
        confirm = input("{} (y/n): ".format(prompt))
        if confirm.lower() == "y":
            decision = True
            break
        elif confirm.lower() == "n":
            decision = False
            break
        else:
            print("Enter 'y' or 'n' for 'yes' or 'no'")

    return decision


def main():
    parser = argparse.ArgumentParser(description="Client for facilitating large-scale data ingest",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="Visit https://docs.theBoss.io for more details")

    parser.add_argument("--api-token", "-a",
                        default=None,
                        help="Token for API authentication. If not provided and ndio is configured those credentials will automatically be used.")
    parser.add_argument("--job-id", "-j",
                        default=None,
                        help="ID of the ingest job if joining an existing ingest job")
    parser.add_argument("--log-file", "-l",
                        default=None,
                        help="Absolute path to the logfile to use")
    parser.add_argument("--cancel", "-c",
                        action="store_true",
                        default=None,
                        help="Flag indicating if you'd like to cancel (and remove) an ingest job. This will not delete data already ingested, but will prevent continuing this ingest job.")
    parser.add_argument("config_file", help="Path to the ingest job configuration file")

    args = parser.parse_args()

    # Make sure you have a config file
    if args.config_file is None:
        parser.print_usage()
        print("Error: Ingest Job Configuration File is required")
        sys.exit(1)

    # Create an engine instance
    try:
        engine = Engine(args.config_file, args.api_token, args.job_id)
    except ConfigFileError as err:
        print("ERROR: {}".format(err))
        sys.exit(1)

    if args.cancel:
        # Trying to cancel
        if args.job_id is None:
            parser.print_usage()
            print("Error: You must provide an ingest job ID to cancel")
            sys.exit(1)

        if not get_confirmation("Are you sure you want to cancel ingest job {}? ".format(args.job_id)):
            print("Command ignored. Job not cancelled")
            sys.exit(0)

        engine.cancel()
        print("Ingest job {} successfully cancelled.".format(args.job_id))
        sys.exit(0)

    else:
        # Trying to create or join an ingest
        if args.job_id is None:
            # Creating a new session - make sure the user wants to do this.
            if not get_confirmation("Would you like to create a NEW ingest job?"):
                # Don't want to create a new job
                print("Ingest job cancelled")
                sys.exit(0)
        else:
            # Resuming a session - make sure the user wants to do this.
            if not get_confirmation("Are you sure you want to resume ingest job {}?".format(args.job_id)):
                # Don't want to resume
                print("Ingest job cancelled")
                sys.exit(0)

    # TODO: Add channel creation
    # Create Channel if needed

    # Setup engine instance.  Prompt user to confirm things if needed
    question_msgs = engine.setup(args.log_file)
    if question_msgs:
        for msg in question_msgs:
            if not get_confirmation(msg):
                print("Ingest job cancelled")
                sys.exit(0)

    if args.job_id is None:
        # Create job
        engine.create_job()
        print("Successfully Created Ingest Job ID: {}".format(engine.ingest_job_id))
        print("Note: You need this ID to continue this job later!")

        if not get_confirmation("Do you want to start uploading now?"):
            print("OK - Your job is ready and waiting for you. You can resume by providing Ingest Job ID '{}' to the client".format(engine.ingest_job_id))
            sys.exit(0)

        # Join job
        engine.join()

    else:
        # Join job
        engine.join()

    # Start it up!
    engine.run()


if __name__ == '__main__':
    main()
