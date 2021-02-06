# Boss Ingest Client Changelog

## 0.11.0

* Added an optional `--ramp_seconds` / `-r` flag to delay the creation of parallel processes. A ramp value of 10 will wait ten seconds in between creating new processes. This is helpful when the source of the ingest data needs time to scale (e.g. Google Cloud Storage).

## 0.10.0

*Dropping* support for Python 2

### Implemented Enhancements:

* New ingest complete functionality implemented


## 0.9.15

### Fixed Bug:

*  Removed delayed wait when requesting job credentials. 

### Implemented Enhancements:

* Added exponential backoff on retries when a client is requesting job status of an ingest job.


## 0.9.14

### Implemented Enhancements:

* Added exponential backoff on retries when a client is trying to join an ingest job.

## 0.9.13

### Fixed Bug:

*  Stop client from creating tile index queue when doing volumetric ingest as that is handled by endpoint.

## 0.9.12

### Implemented Enhancements:

* Added ability to volumetric ingest jobs using Princeton's CloudVolume data format.


## 0.9.11

### Fixed Bug:

* Ingest-job will not be completed unless the upload-queue has zero messages in it.  If an error occurs job will remain now and not close. 


### Implemented Enhancements:

* Added no_cache option to the intern plugin, this will increase the download speed be avoiding the cache.



## 0.9.10

### Fixed Bug:

* Added x and y tile sizes to metadata of s3 tiles.  This allows the ingest backend to create blank tiles when corrupt images are received. 

### Implemented Enhancements:

* Added in prep_ingest.py which will become a new script prep_ingest on pip install.  It will create Collection, Experiment, Coordinate Frame, and Channel for you and add them to your ingest-client json config file.


## 0.9.9

### Fixed Bug:

* Adjusted requirements.txt versions. 

### Implemented Enhancements:

* Added ability to wait for server to verify job has all tiles uploaded before completing. 

## 0.9.8

### Fixed Bug:

* updated requirements.txt to be compatible with boss-manage and intern by removing pinned versions of libraries.

## 0.9.7

### Fixed Bug:

* Fixed an off by one error while looping for credentials renewal


## 0.9.6

### Fixed Bug:

* Updated to catch AccessDenied and InvalidAccessKeyId errors and then request new credentials when this occurs.

## 0.9.5

### Fixed Bug:

* The try-catch in Engine.run() was just around the ingest client’s IO to S3, but user developed plugins can be doing IO to anything and have random, transient errors as well. Moved the try statement to the top of the loop.

## 0.9.4

### Implemented Enhancements:

* Added automatic completion of ingest jobs. When all tiles have been successfully uploaded the job is marked as `Complete` and resources are cleaned up

### Fixed Bugs:

* Fixed error that printed the wrong tile count estimates when the Ingest Job extents contained an offset

### Merged Pull Requests:

- PR5: Allow injection of Configuration objects into Engine - Merged with minor modifications
    - Makes it easier for users to programmatically run the ingest client if many configuration files or parameter changes are needed
    - Add the ability to instantiate an Engine instance directly with a Configuration instance that can be easily manipulated


## 0.9.0

### Implemented Enhancements:

* Added CHANGELOG.md
* **IMPORTANT** Refactored client for pip installation capability
    - refactored `ingest` library to `ingestclient`
    - refactored `client.py` to `boss-ingest`
    - Note: existing ingest job configuration files that were loading built-in plugins from the boss-ingest package will need to be updated from `ingest` to `ingestclient`
* Added pip install capability (`pip install boss-ingest`)
* Added `--force` `-f` flag to automatically accept all prompts (useful for launching on remote nodes)
* Added initial feedback for job progress
* Added `-log_level` `-v` flag to indicate desired log level. Defaults to `warning`
* Added additional feedback on the job that is about to be created when starting a new job
* Added small delay as worker processes spin up to ramp Lambda gracefully

### Fixed Bugs:
* Reduced excessive printing

### Merged Pull Requests:
