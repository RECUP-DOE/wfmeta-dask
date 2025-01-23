import os
from setuptools import setup, find_packages

root = os.path.dirname(__file__) or '.'
with open('%s/requirements.txt' % root, encoding='utf-8') as reqs:
    requirements = reqs.readlines()

setup_args = {
    # informational
    'name' : 'daskcapture',
    'version' : '1.0',
    'description' : 'Python package for collecting + restructuring Mofka-Dask output metadata.',

    # package def
    'package_dir' : {'':'src'},
    'packages' : ['daskcapture'],

    # requirements
    'python_requires' : '>=3.8',
    'data_files' : [('', ['requirements.txt'])],
    'install_requires' : requirements,

    # make it so you can run from the command line
    'entry_points' : {
        'console_scripts': [
            'daskcapture = daskcapture:dask_capture.run',
        ]
    }
}

setup(**setup_args)