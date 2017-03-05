from setuptools import setup, find_packages
from codecs import open
from os import path
from ingestclient import __version__

# to update
# python setup.py sdist bdist_wheel
# twine upload --skip-existing dist/*


here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# get the dependencies and installs
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if x.startswith('git+')]

setup(
    name='boss-ingest',
    version=__version__,

    description='Ingest client for the Boss',
    long_description=long_description,

    install_requires=install_requires,
    dependency_links=dependency_links,
    
    author='Johns Hopkins University Applied Physics Laboratory',
    author_email='iarpamicrons@jhuapl.edu',

    entry_points={
        'console_scripts': ['boss-ingest=ingestclient.client:main'],
    },
    #packages=find_packages('ingestclient'),
    packages=['ingestclient',
              'ingestclient.core',
              'ingestclient.plugins',
              'ingestclient.utils',
              'ingestclient.configs',
              'ingestclient.schema'],
    package_data={
        '': ['*.json'],
    },
    include_package_data=True,

    url='https://github.com/jhuapl-boss/ingest-client',
    #download_url='https://github.com/jhuapl-boss/ingest-client/tarball/' + __version__,

    license='Apache 2.0',
    classifiers=[
      'Development Status :: 4 - Beta',
      'Programming Language :: Python :: 3.5',
      'Programming Language :: Python :: 3.4',
      'Programming Language :: Python :: 2.7',
    ],
    keywords=[
        'brain',
        'microscopy',
        'neuroscience',
        'connectome',
        'connectomics',
        'spatial',
        'EM',
        'electron',
        'calcium',
        'database',
        'boss',
        'microns',
        'iarpa',
        'jhu'
    ]
)
