# Ingest Client
A Python command line application for performing distributed ingest of image data into the Boss 

[![theBoss.io](https://img.shields.io/badge/visit-theBoss.io-blue.svg)](https://docs.theboss.io/)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Code Climate](https://codeclimate.com/github/jhuapl-boss/ingest-client/badges/gpa.svg)](https://codeclimate.com/github/jhuapl-boss/ingest-client)
[![Coverage Status](https://coveralls.io/repos/github/jhuapl-boss/ingest-client/badge.svg?branch=master)](https://coveralls.io/github/jhuapl-boss/ingest-client?branch=master)
[![CircleCI](https://circleci.com/gh/jhuapl-boss/ingest-client/tree/master.svg?style=svg)](https://circleci.com/gh/jhuapl-boss/ingest-client/tree/master)

## Overview
The ingest client application lets users move data from local storage into the Boss, quickly and reliably. It supports both Python 2 and 3.

## Installation

- `mkdir` and `cd` to a directory of your choice

- Clone the ingest client
	
	```
	git clone https://github.com/jhuapl-boss/ingest-client.git
	```
- The ingest client uses [Pillow](http://pillow.readthedocs.io/en/3.4.x/) to handle image files.  Install Pillow dependencies 
	- OSX (using [Homebrew](http://brew.sh/index.html))
	
		```
		brew install libjpeg
		brew install libtiff
		```
	
	- Linux (Ubuntu)

		```
		sudo apt-get install libjpeg-dev libtiff5-dev zlib1g-dev libfreetype6-dev liblcms2-dev libopenjpeg-dev
		```
	
	- Windows - Untested

- Use virtualenv to isolate the ingest client from your system Python installation

	- Using [virtualenv](https://virtualenv.pypa.io/en/stable/):
	
	```
	virtualenv ingest-env
	. ingest-env/bin/activate
	```
	
	- Using [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/):
	
	```
	mkvirtualenv ingest-env
	```
	
- Install Python dependencies
	
	```
	cd ./ingest-client
	pip install -r requirements.txt
	```



## Testing
The nose2 library is used to run unit tests.  From the `ingest-client` directory simply invoke nose2.

```
nose2
```

We use continuous integration to automatically run tests as well.  Future work will expand on testing and add more complex integration testing.


## Configuring Credentials
You must provide the ingest client with your Boss API token so it can make authenticated requests on your behalf. 

Also remember that you must have write permissions to the resource (`collection`, `experiment`, and `channel`) where data is to be written, as specified in the ingest job configuration file. If you created the resources you will automatically have access.

There are four ways to provide your API token to the ingest client.  The ingest client will try to use the first token it finds in the following order:

1. **Via command line arguments**
	- You can directly pass your token to the ingest client when starting it from the command line. See the *Usage* section below.
	
2. **Via the token.json file**
	- Copy the example token.json file
	
		```
		cp ./token.json.example ./token.json 
		```
	- Get your API token.  This can be done by visiting the [Boss Management Console](https://api.theboss.io). After logging in, click on your username in the top right corner, then "API Token".
	
	- Copy your API token to the token.json file, replacing `<insert_token_here>`

3. **Via the intern configuration file**
	- If you have already installed [intern](https://github.com/jhuapl-boss/intern) and added your API token to its configuration file, the ingest client will automatically load the token

4. **Via the intern environment variables**
	- The ingest client can also reuse environment variables used to configure _intern_ to set your API token
	
	```
	  export INTERN_TOKEN=gjwb837js823gd9819ba18894mf94949ecc45
	```

## Usage

An ingest job is the act of uploading a dataset or sub-region of a dataset to the Boss.  You do not need to upload an entire dataset at once if desired, and can specify in both space and time what data is to be written.

There are three operations you can perform with the ingest client - Create, Join, and Cancel an ingest job

- **Creating a NEW Ingest Job**
	- Populate an ingest job configuration file to specify the correct plugins for your data, the Boss resource to use, the extent of the dataset to be ingested, and the tile size.
	- Refer to the [Creating Configuration Files](https://github.com/jhuapl-boss/ingest-client/wiki/Creating-Ingest-Job-Configuration-Files) wiki page for more detail on how to do this. Also, currently a helper script can be found [here](https://github.com/jhuapl-boss/demos/tree/master/ingest_helpers), that will eventually be pulled into the client.

	- Assuming you have created a file, simply call the ingest client
	 
		```
		python client.py <absolute_path_to_config_file>
		```
	- After creating the new Ingest Job, the client will print the ingest job ID and it will be also logged.  

**Remember this ID if you wish to restart the client or run the client on additional nodes for increased throughput**
	
- **Joining an EXISTING Ingest Job**
	- You can join an existing ingest job and start uploading data any time after it has been created. This can be useful if the client has crashed, or if you want to run the client on additional nodes in parallel.
	
		```
		python client.py <absolute_path_to_config_file> --job-id <ingest_job_id>
		```
		or
		
		```
		python client.py <absolute_path_to_config_file> -j <ingest_job_id>
		```

- **Cancelling an Ingest Job**
	-	Sometimes you may want to stop an ingest job. You can do this by "cancelling" it.  Currently this will delete all tiles that have been uploaded but not ingested into the Boss yet.  Any data that made its way through the ingest pipeline will remain.  Also temporary queues will be purged and deleted.

		```
		python client.py --cancel --job-id <ingest_job_id>
		```
		or
		
		```
		python client.py -c -j <ingest_job_id>
		```
 
        If you are working with the non-production Boss instance (api.theboss.io), then you can provide a configuration file specifying the desired host as the commands shown above will default to the production Boss environment. 
		
		```
		python client.py <absolute_path_to_config_file> -c -j <ingest_job_id>
		```

- **Multiprocessing**
	-   You can choose to have multiple upload engines start in parallel processes by setting the `-p` argument as outlined in the example below. (Default number of upload processes = 1)

		```
		python client.py <absolute_path_to_config_file> --processes_nb <number_of_processes>
		```
		or
		
		```
		python client.py <absolute_path_to_config_file> -p <number_of_processes>
		```



## Plugins

To enable support for many different ways to organize data and many different types of file formats, plugins are used to convert tile indices to a file handle for uploading.  Some plugins have been initially provided as an example:

- [CATMAID](http://catmaid.readthedocs.io/en/stable/tile_sources.html) File-based image stack
	- `path_processor``class` = "ingest.plugins.filesystem.CatmaidFileImageStackPathProcessor"
	- `tile_processor``class` = "ingest.plugins.filesystem.CatmaidFileImageStackTileProcessor"
	- An example configuration file using this plugin is `ingest/configs/boss-v0.1-catmaid-file-stack-example.json`
	- This plugin assumes the data is organized as a CATMAID file-based image stack (type 1 on their docs page). You must provide the `filetype` (e.g. "png") and the `root_dir` as custom parameters

- Multi-page TIFF image 
	- `path_processor``class` = "ingest.plugins.multipage_tiff.SingleTimeTiffPathProcessor"
	- `tile_processor``class` = "ingest.plugins.multipage_tiff.SingleTimeTiffTileProcessor"
	- An example configuration file using this plugin is `/ingest/configs/boss-v0.1-time-series-example.json`
	- This plugin is for time-series calcium imaging data.  It assumes each z-slice is stored in a single multipage TIFF file where consecutive pages are consecutive time samples.  You must provide custom parameters indicating the `filetype`, `datatype`, and location to each file where the key is "z_<slice_index>" and value is the absolute path. 


Additional plugins can be added to the client as needed.  For more details on how to create your own plugin check out the  [Creating Custom Plugins](https://github.com/jhuapl-boss/ingest-client/wiki/Creating-Custom-Plugins) page.


## License
If not otherwise marked, all code in this repository falls under the license granted in LICENSE.md.