#!/usr/bin/env python
import os
import os.path
import re
from setuptools import (find_packages, setup)

VERSION_RE = re.compile('__version__\s*=\s*[\'"](.*)[\'"]')

def get_version():
    with open(os.path.join(
          os.path.dirname(os.path.abspath(__file__)),
          'airflow_aws_cost_explorer/plugin.py')) as f:
        for line in f:
            match = VERSION_RE.match(line)
            if match:
                return match.group(1)
    raise Exception

with open('README.md', 'r') as f:
    long_description = f.read()

with open('requirements.txt', 'r') as f:
    install_requires = f.read().split('\n')

setup(
    name='airflow_aws_cost_explorer',
    version=get_version(),
    packages=find_packages(),
    include_package_data=True,
    entry_points = {
        'airflow.plugins': [
            'airflow_aws_cost_explorer = airflow_aws_cost_explorer.plugin:AWSCostExplorerPlugin'
        ]
    },
    zip_safe=False,
    url='https://github.com/andreax79/airflow-aws-cost-explorer',
    author='Andrea Bonomi',
    author_email='andrea.bonomi@gmail.com',
    description='Apache Airflow Operator exporting AWS Cost Explorer data to local file or S3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=install_requires,
    license='Apache License, Version 2.0',
    classifiers=[
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
    ]
)

