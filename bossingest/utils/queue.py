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
import time

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

    def restore_messages(self, input_dir):
        """Method to re-load a backed up messages to an ingest queue"""
        for msg_file in os.listdir(input_dir):
            print(msg_file)
            with open(os.path.join(input_dir, msg_file), "rt") as msg:
                msg_body = msg.read()
                response = self.queue.send_message(MessageBody=msg_body)
                if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    print("failed to upload {}".format(msg_file))

    def invoke_ingest(self, input_dir, x_tile, y_tile):
        """Method to trigger lambda functions until ingest completes"""
        # Load a single message to build the object metadata
        filename = [x for x in os.listdir(input_dir)][0]
        with open(os.path.join(input_dir, filename), "rt") as msg:
            metadata = json.load(msg)

        metadata["tile_size_x"] = x_tile
        metadata["tile_size_y"] = y_tile
        metadata["lambda-name"] = "ingest"

        # Get how many to invoke
        starting_message_count = int(self.queue.attributes['ApproximateNumberOfMessages'])
        num_invocations = range(0, starting_message_count)
        print("Triggering {} lambdas".format(starting_message_count))

        # Invoke Ingest lambda functions
        lambda_client = boto3.client('lambda', region_name="us-east-1")
        cnt = 0
        for _ in num_invocations:
            lambda_client.invoke(FunctionName=metadata["parameters"]["ingest_lambda"],
                                 InvocationType='Event',
                                 Payload=json.dumps(metadata).encode())
            cnt += 1
            if cnt > 30:
                print("Invoked 30...throttling...")
                time.sleep(5)
                cnt = 0

        print("Waiting for 2.5 minutes message timeout to check outcome...")
        time.sleep(150)
        print("Started with {} messages. Resulted in {} messages".format(starting_message_count,
                                                                         self.queue.attributes['ApproximateNumberOfMessages']))
