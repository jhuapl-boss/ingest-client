__version__ = '0.9.5'


def check_version():
    """
    Tells you if you have an old version of boss-ingest.
    """
    import requests
    r = requests.get('https://pypi.python.org/pypi/boss-ingest/json').json()
    r = r['info']['version']
    if r != __version__:
        print("You are using version {}. A newer version of boss-ingest is available: {} ".format(__version__, r) +
              "\n\n'pip install -U boss-ingest' to update.")
    else:
        print("You are using version {}, which is the latest available.".format(__version__))

