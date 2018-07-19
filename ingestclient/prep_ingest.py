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

"""
This is a convenience script that ensures that the specified Boss resources
(collection, experiment, channel, and coordinate frame) exist before using
boss-ingest.  It takes a JSON input file that describes each Boss resource.
Note that if the Boss resource already exists, it will not be modified.  For
example, if the coordinate frame already exists, the properties specified in
the JSON input file will be ignored.  If the Boss resource does not exist, it
is created according to the specification in the input file.

The boss-ingest config file that is referenced by this script's JSON input
file is optionally updated with the name of the collection, experiment, and
channel.  This property is "ingest_cfg" and is only required when using the
`--writecfg` option.

Below is a sample JSON input file for this script.  If a property is not marked
with an "Optional" comment, it must be included in the JSON file.  In the 
sample JSON, below, the values assigned to the optional properties are the
default values that will be used if you do not provide these properties.  The
exception is "ingest_cfg" as explained previously.

{
  "ingest_cfg": "ingest.json",      # boss-ingest config associated with this.
  "collection": {
    "name": "my_collection",
    "description": "Example collection"
  },
  "coordinate_frame": {
    "name": "my_coord",
    "description": "Example coord frame",
    "x_start": 0,
    "x_stop": 1024,
    "y_start": 0,
    "y_stop": 1024,
    "z_start": 0,
    "z_stop": 32,
    "voxel_size_x": 4,
    "voxel_size_y": 4,
    "voxel_size_z": 40
  },
  "experiment": {
    "name": "my_exp",
    "description": "Example experiment",
    "num_time_samples": 1,                  # Optional.
    "num_hierarchy_levels": 1,              # Optional.
    "hierarchy_method": "anisotropic",      # Optional.
    "time_step": 0,                         # Optional.
    "time_step_unit": "nanoseconds"         # Optional.
  },
  "channel": {
    "name": "my_chan",
    "description": "Example channel",
    "type": "image",
    "datatype": "uint8",
    "sources": []                           # Optional names of source channels.
  }
}
"""

import argparse
from intern.remote.boss import BossRemote
from intern.resource.boss.resource import *
import json
import os


# Name of group to assign read-only privileges to.
GOV_TEAM = 'gov_team'

def create_collection(config, rmt):
    """
    Ensure collection exists.

    Args:
        config (dict):
        rmt (BossRemote):

    Returns:
        (CollectionResource)
    """
    collection = CollectionResource(
        config['collection']['name'], config['collection']['description'])
    try:
        collection = rmt.create_project(collection)
    except Exception as e:
        collection = rmt.get_project(collection)
    return collection


def create_experiment(config, rmt, collection, coord):
    """
    Ensure channel exists.

    Args:
        config (dict):
        rmt (BossRemote):
        collection (CollectionResource):
        coord (CoordinateFrameResource):

    Returns:
        (ExperimentResource)
    """

    # Default values if not provided in config file.
    exp_vals = {
        'hierarchy_method': 'anisotropic',
        'num_hierarchy_levels': 1,
        'num_time_samples': 1,
        'time_step': 0,
        'time_step_unit': 'seconds'
    }

    exp_vals.update(config['experiment'])

    experiment = ExperimentResource(
        config['experiment']['name'], collection.name, coord.name,
        config['experiment']['description'],
        num_hierarchy_levels=exp_vals['num_hierarchy_levels'],
        hierarchy_method=exp_vals['hierarchy_method'],
        num_time_samples=exp_vals['num_time_samples'],
        time_step=exp_vals['time_step'],
        time_step_unit=exp_vals['time_step_unit'])
    try:
        experiment = rmt.create_project(experiment)
    except:
        experiment = rmt.get_project(experiment)
    return experiment


def create_channel(config, rmt, collection, experiment):
    """
    Ensure channel exists.

    Args:
        config (dict):
        rmt (BossRemote):
        collection (CollectionResource):
        experiment (ExperimentResource):

    Returns:
        (ChannelResource)
    """
    if 'sources' not in config['channel']:
        sources = []
    else:
        sources = config['channel']['sources']

    channel = ChannelResource(
        config['channel']['name'], collection.name, experiment.name,
        type=config['channel']['type'], description=config['channel']['description'],
        datatype=config['channel']['datatype'], sources=sources)
    try:
        channel = rmt.create_project(channel)
    except:
        channel = rmt.get_project(channel)
    return channel


def create_coord_frame(config, rmt):
    """
    Ensure coordinate frame exists.

    Args:
        config (dict):
        rmt (BossRemote):

    Returns:
        (CoordinateFrameResource)
    """
    coord = CoordinateFrameResource(config['coordinate_frame']['name'],
                                    config['coordinate_frame']['description'],
                                    config['coordinate_frame']['x_start'],
                                    config['coordinate_frame']['x_stop'],
                                    config['coordinate_frame']['y_start'],
                                    config['coordinate_frame']['y_stop'],
                                    config['coordinate_frame']['z_start'],
                                    config['coordinate_frame']['z_stop'],
                                    config['coordinate_frame']['voxel_size_x'],
                                    config['coordinate_frame']['voxel_size_y'],
                                    config['coordinate_frame']['voxel_size_z'])
    try:
        coord = rmt.create_project(coord)
    except:
        coord = rmt.get_project(coord)
    return coord


def add_gov_team_permissions(rmt, collection, experiment, channel):
    """
    Give read permission to the government team.

    Args:
        rmt (BossRemote):
        collection (CollectionResource):
        experiment (ExperimentResource):
        channel (ChannelResource):
    """

    try:
        rmt.add_permissions(GOV_TEAM, collection, ['read'])
    except:
        print('Failed to automatically add {} group to collection: {}'.format(GOV_TEAM, collection.name))

    try:
        rmt.add_permissions(GOV_TEAM, experiment, ['read'])
    except:
        print('Failed to automatically add {} group to experiment: {}'.format(GOV_TEAM, experiment.name))
    try:
        rmt.add_permissions(GOV_TEAM, channel, ['read'])
    except:
        print('Failed to automatically add {} group to channel: {}'.format(GOV_TEAM, channel.name))


def main(parser_args=None):
    """
    Main entry point of script.

    Args:
        parser_args(argparse.ArgumentParser): A pre-loaded ArgumentParser instance
    """
    if parser_args is None:
        args = parse_args()
    else:
        args = parser_args

    rmt = BossRemote(args.intern_cfg)

    # Load Configuration File
    with open(args.config_file, 'rt') as cfg:
        config = json.load(cfg)

    collection = create_collection(config, rmt)
    coord = create_coord_frame(config, rmt)
    experiment = create_experiment(config, rmt, collection, coord)
    channel = create_channel(config, rmt, collection, experiment)

    if args.govteam:
        add_gov_team_permissions(rmt, collection, experiment, channel)

    # Update boss-ingest config file with resources names from config file
    if args.writecfg:
        with open(config['ingest_cfg'], 'rt') as cfg:
            ingest_file = json.load(cfg)
            if 'database' not in ingest_file:
                ingest_file['database'] = {}
            ingest_file['database']['collection'] = collection.name
            ingest_file['database']['experiment'] = experiment.name
            ingest_file['database']['channel'] = channel.name

        with open(config['ingest_cfg'], 'wt') as cfg:
            json.dump(ingest_file, cfg, indent=2)

    print('\n\nRun this command in the ingest-client repo directory to execute the ingest client:')
    print('\n  export INTERN_TOKEN={}'.format(rmt.project_service.auth))
    print('  boss-ingest {}'.format(os.path.abspath(config['ingest_cfg'])))


def parse_args():
    """
    Parse command line arguments.

    Returns:
        (Namespace)
    """
    parser = argparse.ArgumentParser(
        description='Script that creates the collection/experiment/channel for ingest.  ' + 
        'To supply arguments from a file, provide the filename prepended with an `@`.',
        fromfile_prefix_chars = '@')
    parser.add_argument(
        '--intern_cfg', '-i',
        default='boss.cfg',
        help='intern config file')
    parser.add_argument(
        '--writecfg',
        action='store_true',
        help='Update the boss-ingest config file with names of collection/experiment/channel.')
    parser.add_argument(
        '--govteam',
        action='store_true',
        help='Add read-only permissions for the Government team.')
    parser.add_argument(
        'config_file',
        help='Collection/experiment/channel configuration in JSON.')
    return parser.parse_args()


if __name__ == '__main__':
    main()

