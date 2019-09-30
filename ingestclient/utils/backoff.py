# Copyright 2019 The Johns Hopkins University Applied Physics Laboratory
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

from random import randint

def get_wait_time(retry_num):
    """
    Compute time for exponential backoff.

    Args:
        retry_num (int): Retry attempt number to determine wait time.

    Returns:
        (int): Amount of time to wait.
    """
    return 2 ** (retry_num+3)

def get_wait_time_rand(retry_num):
    """
    Compute time for exponential backoff using a random element.

    Args:
        retry_num (int): Retry attempt number to determine random number range.

    Returns:
        (int): Amount of time to wait.
    """
    return randint(1, 2 ** (retry_num+3))
