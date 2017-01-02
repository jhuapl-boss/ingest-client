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
import boto3
import os
import json


class QueueRecovery(object):
    """Class to manage recovering data from a queue"""

    def __init__(self, queue_name, region="us-east-1"):
        self.sqs = boto3.resource('sqs', region_name=region)
        self.queue = self.sqs.Queue(url=queue_name)

    def simple_store_messages(self, output_dir):
        """Method to store all remaining messages in a queue for later use during a recovery/debug operation

        Currently this assumes you can download all messages BEFORE the visibility timeout. Otherwise you will enter an
        endless loop. For large number of messages, additional development will be needed.

        Args:
            output_dir(str): directory to dump data

        Returns:
            None
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        cnt = 0
        while True:
            msgs = self.queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=10)

            if msgs:
                for msg in msgs:
                    cnt += 1
                    with open(os.path.join(output_dir, "{}.json".format(msg.message_id)), "wt") as msg_file:
                        msg_file.write(msg.body)
            else:
                break

        print("Saved {} messages to {}.".format(cnt, output_dir))

