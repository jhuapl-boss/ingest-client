# Boss Ingest Client Changelog


## 0.9.0

###Implemented Enhancements:

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

###Fixed Bugs:
* Reduced excessive printing

###Merged Pull Requests: