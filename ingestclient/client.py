#!/usr/bin/env python
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

from ingestclient.core.engine import Engine
from ingestclient.core.config import ConfigFileError
from ingestclient.core.backend import BossBackend
from ingestclient import check_version
from ingestclient.utils.log import always_log_info
from ingestclient.utils.console import print_estimated_job

from six.moves import input
import datetime
import argparse
import sys
import multiprocessing as mp
import os
import time
import logging


def get_confirmation(prompt, force=False):
    """Method to confirm decisions

    Args:
        prompt(str): Question to ask the user
        force(bool): Flag indicating if user prompts should be ignored

    Returns:
        (bool): True indicating yes, False indicating no
    """
    if not force:
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
    else:
        return True


def worker_process_run(api_token, job_id, pipe, config_file=None, configuration=None):
    """A worker process main execution function. Generates an engine, and joins the job
       (that was either created by the main process or joined by it).
       Ends when no more tasks are left that can be executed.

    Args:
        api_token(str): the token to initialize the engine with.
        job_id(int): the id of the job the engine needs to join with.
        pipe(multiprocessing.Pipe): the receiving end of the pipe that communicates with the master process.
        config_file(str): the path to the configuration file (configuration required if omitted)
        configuration(Configuration): a pre-loaded configuration object (config_file required if omitted)

    """
    always_log_info("Creating new worker process, pid={}.".format(os.getpid()))

    # Create the engine
    if config_file is None and configuration is None:
        raise Exception('Must provide either a configuration instance or a configuration file')

    try:
        engine = Engine(config_file=config_file, 
                        configuration=configuration,
                        backend_api_token=api_token, 
                        ingest_job_id=job_id)
    except ConfigFileError as err:
        print("ERROR (pid: {}): {}".format(os.getpid(), err))
        sys.exit(1)

    # Join job
    engine.join()

    # Start it up!
    should_run = True
    while should_run:
        try:
            engine.run()
            # run will end if no more jobs are available
            should_run = False
        except KeyboardInterrupt:
            # Make sure they want to stop this client, wait for the main process to send the next step
            should_run = pipe.recv()
    always_log_info("  - Process pid={} finished gracefully.".format(os.getpid()))
    

def get_parser():
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
    parser.add_argument("--log-level", "-v",
                        default="warning",
                        help="Log level to use: critical, error, warning, info, debug")
    parser.add_argument("--version",
                        action="store_true",
                        default=False,
                        help="Get the package version")
    parser.add_argument("--cancel", "-c",
                        action="store_true",
                        default=None,
                        help="Flag indicating if you'd like to cancel (and remove) an ingest job. This will not delete data already ingested, but will prevent continuing this ingest job.")
    parser.add_argument("--force", "-f",
                        action="store_true",
                        default=False,
                        help="Flag indicating if you'd like ignore all confirmation prompts.")
    parser.add_argument("--manual-complete", "-m",
                        action="store_true",
                        default=False,
                        help="Flag indicating if you want to manually mark an Ingest Job for completion. If omitted, the client will automatically cleanup after a successful upload")
    parser.add_argument("--processes_nb", "-p", type=int,
                        default=1,
                        help="The number of client processes that will upload the images of the ingest job.")
    parser.add_argument("config_file", nargs='?', help="Path to the ingest job configuration file")

    return parser


def main(configuration=None, parser_args=None):
    """Client UI main

    Args:
        configuration(ingestclient.core.config.Configuration): A pre-loaded configuration instance
        parser_args(argparse.ArgumentParser): A pre-loaded ArgumentParser instance

    Returns:

    """
    parser = get_parser()
    if parser_args is None:
        args = parser.parse_args()
    else:
        args = parser_args

    # Get the version
    if args.version:
        check_version()
        return

    # Make sure you have a config file
    if args.config_file is None and configuration is None:
        if args.cancel:
            # If no config is provided and you are deleting, the client defaults to the production Boss stack
            boss_backend_params = {"client": {
                "backend": {
                    "name": "boss",
                    "class": "BossBackend",
                    "host": "api.theboss.io",
                    "protocol": "https"}}}
            backend = BossBackend(boss_backend_params)
            backend.setup(args.api_token)

            # Trying to cancel
            if args.job_id is None:
                parser.print_usage()
                print("Error: You must provide an ingest job ID to cancel")
                sys.exit(1)

            if not get_confirmation("Are you sure you want to cancel ingest job {}? ".format(args.job_id), args.force):
                print("Command ignored. Job not cancelled")
                sys.exit(0)

            backend.cancel(args.job_id)
            print("Ingest job {} successfully cancelled.".format(args.job_id))
            sys.exit(0)
        else:
            # Not deleting, so you need a config file
            parser.print_usage()
            print("Error: Ingest Job Configuration File is required")
            sys.exit(1)

    # Setup logging
    log_level = logging.getLevelName(args.log_level.upper())
    if not args.log_file:
        # Using default log path
        log_path = os.path.expanduser("~/.boss-ingest")
        log_file = os.path.join(log_path,
                                'ingest_log{}_pid{}.log'.format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"),
                                                                os.getpid()))
        # Make sure the logs dir exists if using the default log path
        if not os.path.exists(log_path):
            os.makedirs(log_path)
    else:
        log_file = args.log_file

    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=log_file,
                        filemode='a')
    logging.getLogger('ingest-client').addHandler(logging.StreamHandler(sys.stdout))

    # Create an engine instance
    try:
        engine = Engine(config_file=args.config_file, 
                        backend_api_token=args.api_token, 
                        ingest_job_id=args.job_id, 
                        configuration=configuration)
    except ConfigFileError as err:
        print("ERROR: {}".format(err))
        sys.exit(1)

    if args.cancel:
        # Trying to cancel
        if args.job_id is None:
            parser.print_usage()
            print("Error: You must provide an ingest job ID to cancel")
            sys.exit(1)

        if not get_confirmation("Are you sure you want to cancel ingest job {}? ".format(args.job_id), args.force):
            print("Command ignored. Job not cancelled")
            sys.exit(0)

        always_log_info("Attempting to cancel Ingest Job {}.".format(args.job_id))
        engine.cancel()
        always_log_info("Ingest job {} successfully cancelled.".format(args.job_id))
        sys.exit(0)

    else:
        # Trying to create or join an ingest
        if args.job_id is None:
            # Creating a new session - make sure the user wants to do this.
            print_estimated_job(config_file=args.config_file, configuration=configuration)
            print("\n")
            if not get_confirmation("Would you like to create a NEW ingest job?", args.force):
                # Don't want to create a new job
                print("Exiting")
                sys.exit(0)
        else:
            # Resuming a session - make sure the user wants to do this.
            if not get_confirmation("Are you sure you want to resume ingest job {}?".format(args.job_id), args.force):
                # Don't want to resume
                print("Exiting")
                sys.exit(0)

    # Setup engine instance.  Prompt user to confirm things if needed
    question_msgs = engine.setup()
    if question_msgs:
        for msg in question_msgs:
            if not get_confirmation(msg, args.force):
                print("Ingest job cancelled")
                sys.exit(0)

    if args.job_id is None:
        # Create job
        engine.create_job()
        always_log_info("Successfully Created Ingest Job ID: {}".format(engine.ingest_job_id))
        always_log_info("Note: You need this ID to continue this job later!")

        if not get_confirmation("\nDo you want to start uploading now?", args.force):
            print("OK - Your job is waiting for you. You can resume by providing Ingest Job ID '{}' to the client".format(engine.ingest_job_id))
            sys.exit(0)

        # Join job
        engine.join()

    else:
        # Join job
        engine.join()

    # Create worker processes
    workers = []
    for i in range(args.processes_nb):
        new_pipe = mp.Pipe(False)
        new_process = mp.Process(target=worker_process_run, 
                                 args=(args.api_token, engine.ingest_job_id, new_pipe[0]),
                                 kwargs={'config_file': args.config_file, 'configuration': configuration}
                                 )
        workers.append((new_process, new_pipe[1]))
        new_process.start()

        # Sleep to slowly ramp up load on lambda
        time.sleep(.5)

    # Start the main process engine
    start_time = time.time()
    should_run = True
    job_complete = False
    while should_run:
        try:
            engine.monitor(workers)
            # run will end if no more jobs are available, join other processes
            should_run = False
            job_complete = True
        except KeyboardInterrupt:
            # Make sure they want to stop this client
            while True:
                quit_uploading = input("Are you sure you want to quit uploading? (y/n)")
                if quit_uploading.lower() == "y":
                    always_log_info("Stopping upload engine.")
                    should_run = False
                    break
                elif quit_uploading.lower() == "n":
                    print("Continuing...")
                    break
                else:
                    print("Enter 'y' or 'n' for 'yes' or 'no'")

            # notify the worker processes that they should stop execution
            for _, worker_pipe in workers:
                worker_pipe.send(should_run)

    always_log_info("Waiting for worker processes to close...\n")
    time.sleep(1)  # Make sure workers have cleaned up
    for worker_process, worker_pipe in workers:
        worker_process.join()
        worker_pipe.close()

    if job_complete:
        # If auto-complete, mark the job as complete and cleanup
        always_log_info("All upload tasks completed in {:.2f} minutes.".format((time.time() - start_time) / 60))
        if not args.manual_complete:
            always_log_info(" - Marking Ingest Job as complete and cleaning up. Please wait.")
            engine.complete()
            always_log_info(" - Cleanup Done")
        else:
            always_log_info(" - Auto-complete disabled. This ingest job will remain in the 'Uploading' state until you manually mark it as complete")
    else:
        always_log_info("Client exiting")
        always_log_info("Run time: {:.2f} minutes.".format((time.time() - start_time) / 60))


if __name__ == '__main__':
    main()
