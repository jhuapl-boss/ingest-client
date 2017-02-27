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
from ingestclient.utils.queue import QueueRecovery
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
    parser = argparse.ArgumentParser(description="Client for debugging the ingest process",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="Visit https://docs.theBoss.io for more details")

    parser.add_argument("queue_name",
                        help="Name of the ingest SQS queue")

    parser.add_argument("data_dir",
                        default=None,
                        help="Data Directory")

    parser.add_argument("--download", "-d",
                        default=None,
                        action="store_true",
                        help="Download all messages in a queue")

    parser.add_argument("--upload", "-u",
                        default=None,
                        action="store_true",
                        help="Upload all messages in a queue")

    parser.add_argument("--invoke", "-i",
                        default=None,
                        action="store_true",
                        help="Trigger lambda invocations to cleanup ingest")

    parser.add_argument("--x_tile_size", "-x",
                        default=None,
                        help="X tile size, needed for re-invoking ingest lambdas")

    parser.add_argument("--y_tile_size", "-y",
                        default=None,
                        help="y tile size, needed for re-invoking ingest lambdas")

    args = parser.parse_args()

    qr = QueueRecovery(args.queue_name)

    if args.download:
        # Trying to download
        print("Downloading messages from {}".format(args.queue_name))
        qr.simple_store_messages(args.data_dir)

    if args.upload:
        # Trying to upload
        print("Uploading messages to {}".format(args.queue_name))
        qr.restore_messages(args.data_dir)

    if args.invoke:
        print("Triggering messages in {}".format(args.queue_name))
        qr.invoke_ingest(args.data_dir, args.x_tile_size, args.y_tile_size)


if __name__ == '__main__':
    main()
