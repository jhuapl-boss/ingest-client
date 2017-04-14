# Boss Ingest Client
A Python command line application for performing distributed ingest of data into the Boss 

[![theBoss.io](https://img.shields.io/badge/visit-theBoss.io-blue.svg)](https://docs.theboss.io/)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Code Climate](https://codeclimate.com/github/jhuapl-boss/ingest-client/badges/gpa.svg)](https://codeclimate.com/github/jhuapl-boss/ingest-client)
[![Coverage Status](https://coveralls.io/repos/github/jhuapl-boss/ingest-client/badge.svg?branch=master)](https://coveralls.io/github/jhuapl-boss/ingest-client?branch=master)
[![CircleCI](https://circleci.com/gh/jhuapl-boss/ingest-client/tree/master.svg?style=svg)](https://circleci.com/gh/jhuapl-boss/ingest-client/tree/master)

## Overview
The ingest client application lets users move data from local storage into the Boss, quickly and reliably. It supports both Python 2 and 3. It uses a JSON configuration file to define ingest jobs, and a plugin system to support any local file organization.

## Installation

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
	
- Install the ingest client

	```
	pip install boss-ingest
	```
	
	If you get errors installing Pillow, it is most likely because you do not have all of Pillow's dependencies installed. Check out the "Installing Pillow Dependencies" section below for help.


## Configuring Credentials
You must provide the ingest client with your Boss API token so it can make authenticated requests on your behalf. 

Also remember that you must have write permissions to the resource (`collection`, `experiment`, and `channel`) where data is to be written, as specified in the ingest job configuration file. If you created the resources you will automatically have access.

There are three ways to provide your API token to the ingest client.  The ingest client will try to use the first token it finds in the following order:

1. **Via command line arguments**
	- You can directly pass your token to the ingest client when starting it from the command line. See the *Usage* section below.

2. **Via the intern environment variables**
	- The ingest client can also reuse environment variables used to configure _intern_ to set your API token

	```
	  export INTERN_TOKEN=<you_token_here>
	```

3. **Via the intern configuration file**
	- If you have already installed [intern](https://github.com/jhuapl-boss/intern) and added your API token to its configuration file, the ingest client will automatically load the token


## Usage

The ingest client is installed as a system script and can be called from the command line directly as `boss-ingest`.

An ingest job is the act of uploading a dataset or sub-region of a dataset to the Boss.  You do not need to upload an entire dataset at once if desired, and can specify in both space and time what data is to be written.

There are three primary operations you can perform with the ingest client: Create, Join, and Cancel an ingest job

- **Creating a NEW Ingest Job**
	- Populate an ingest job configuration file to specify the correct plugins for your data, the Boss resource to use, the extent of the dataset to be ingested, and the tile size.
	- Refer to the [Creating Configuration Files](https://github.com/jhuapl-boss/ingest-client/wiki/Creating-Ingest-Job-Configuration-Files) wiki page for more detail on how to do this. Also, currently a helper script to create a channel in the Boss to which data can be written is found [here](https://github.com/jhuapl-boss/demos/tree/master/ingest_helpers).

	- Assuming you have created a configuration file, simply call the ingest client
	 
		```
		boss-ingest <absolute_path_to_config_file>
		```
	- After creating the new Ingest Job, the client will print the ingest job ID and it will be also logged.  

		_Remember this ID if you wish to restart the client or run the client on additional nodes for increased throughput_
	
	- You have **14 days** to complete uploading the data for this ingest job before the upload work queue automatically gets purged
	
- **Joining an EXISTING Ingest Job**
	- You can join an existing ingest job and start uploading data any time after it has been created. This can be useful if the client has crashed, or if you want to run the client on additional nodes in parallel.
	
		```
		boss-ingest <absolute_path_to_config_file> --job-id <ingest_job_id>
		```
		or
		
		```
		boss-ingest <absolute_path_to_config_file> -j <ingest_job_id>
		```

- **Cancelling an Ingest Job**
	-	Sometimes you may want to stop an ingest job. You can do this by "cancelling" it.  Currently this will delete all tiles that have been uploaded but not ingested into the Boss yet.  Any data that made its way through the ingest pipeline will remain.  Also temporary queues will be purged and deleted.

		```
		boss-ingest --cancel --job-id <ingest_job_id>
		```
		or
		
		```
		boss-ingest -c -j <ingest_job_id>
		```
 
        If you are working with the non-production Boss instance (api.theboss.io), then you can provide a configuration file specifying the desired host as the commands shown above will default to the production Boss environment. 
		
		```
		boss-ingest <absolute_path_to_config_file> -c -j <ingest_job_id>
		```

- **Completing an Ingest Job**
	-	The ingest client now automatically "completes" an ingest job when the upload queue has been completely processed. This operation will ensure that all data has made it into the Boss, clean up temporary resources that have been provisioned by the Boss, and update the status of the ingest job. Note, it can take 5-60 seconds to finish completing a job. 

		If you do **not** want the client to automatically compete the job for you, you can add a flag to disable this functionality, as shown below 

		```
		boss-ingest <absolute_path_to_config_file> --manual-complete
		```
		or
		
		```
		boss-ingest  <absolute_path_to_config_file> -m
		```
 
- **Multiprocessing**
	-  You can choose to have multiple upload engines start in parallel processes by setting the `-p` argument as outlined in the example below. (Default number of upload processes = 1)

		```
		boss-ingest <absolute_path_to_config_file> --processes_nb <number_of_processes>
		```
		or
		
		```
		boss-ingest <absolute_path_to_config_file> -p <number_of_processes>
		```

- **Logging**
	-   You can choose where to write the log file by specifying and absolute file path suing the -l parameter. If omitted, data is logged in `~/.boss-ingest`

		```
		boss-ingest <absolute_path_to_config_file> --log-file <absolute_filename>
		```
		or
		
		```
		boss-ingest <absolute_path_to_config_file> -l <absolute_filename>
		```

	-   You can also control the logging level. By default it is set to `WARNING`, although some important information is forced to always log. The `INFO` level and lower result in very large log files and is not recommended for anything besides development and debug.

		```
		boss-ingest <absolute_path_to_config_file> --log-level <critical|error|warning|info|debug>
		```
		or
		
		```
		boss-ingest <absolute_path_to_config_file> -v <critical|error|warning|info|debug>

		```


## Plugins

To handle the many different ways users can organize and store data, "plugins" are used to perform two operations. The first (Path Processor) is responsible for taking user specified parameters and tile indices provided from the upload task queue to generate an absolute file path to the correct data file associated with the image tile. The second (Tile Processor) is responsible for taking user specified parameters, tile indices, and generated file path to generate a file handle containing the image data. This handle is then used to upload the image tile.

The [ingest client wiki](https://github.com/jhuapl-boss/ingest-client/wiki) on GitHub provides more detailed information on on how to [create plugins](https://github.com/jhuapl-boss/ingest-client/wiki/Creating-Custom-Plugins) and which plugins come [pre-installed](https://github.com/jhuapl-boss/ingest-client/wiki/Plugins).

If you develop your own plugins, you simply need to make sure they are on your `PYTHONPATH` before calling `boss-ingest`

```
export PYTHONPATH=$PYTHONPATH:/<path_to_modules>
```


## Installing Pillow Dependencies

The ingest client uses [Pillow](http://pillow.readthedocs.io/en/latest/installation.html) to handle image files. There are several dependencies you may need to install before you can run `pip install Pillow`. Pillow is installed automatically when you run `pip install boss-ingest`, so these external dependencies must already be installed for that command to successfully complete. 

- OSX
	
	Install jpeg and tiff libraries using [Homebrew](http://brew.sh/index.html)
	
	```
	brew install libjpeg
	brew install libtiff
	```
	
	Sometimes you may also need to install zlib development packages from XCode
	
	```
	xcode-select --install
	```
	
	
- Linux (Ubuntu)

	```
	sudo apt-get install libjpeg-dev libtiff5-dev zlib1g-dev libfreetype6-dev liblcms2-dev libopenjpeg-dev
	```
	
- Windows - Untested



## Installation for Development

- `mkdir` and `cd` to a directory of your choice

- Clone the ingest client
	
	```
	git clone https://github.com/jhuapl-boss/ingest-client.git
	```

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
	
- An additional token configuration method via the token.json file is available if you've cloned the ingest-client repository locally
	
	- Create a token.json file in the root directory of the repo
	
		```
		vi ./token.json 
		```
	- Get your API token.  This can be done by visiting the [Boss Management Console](https://api.theboss.io). After logging in, click on your username in the top right corner, then "API Token".
	
	- Copy your API token to the token.json file that looks like this:

		```
		{
		    "token": "<insert_token_here>",
		    "host": "api.theboss.io"
		}
		```



## Testing
The nose2 library is used to run unit tests.  From the `ingest-client` directory simply invoke nose2.

```
nose2
```

We use continuous integration to automatically run tests as well.  Future work will expand on testing and add more complex integration testing.


## License
If not otherwise marked, all code in this repository falls under the license granted in LICENSE.md.
