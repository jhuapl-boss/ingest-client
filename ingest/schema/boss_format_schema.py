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
from pkg_resources import resource_filename
import os

# Simple Script to format a schema file for posting to the Boss via the admin/API browser
schema_file = os.path.join(resource_filename("ingest", "schema"), "boss-v0.1-schema.json")

with open(schema_file, 'r') as file_handle:
    data = json.load(file_handle)

post_str = json.dumps(data)

print(post_str)
