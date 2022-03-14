# Copyright (c) 2022 RWTH Aachen - Werkzeugmaschinenlabor (WZL)
# Contact: Simon Cramer, s.cramer@wzl-mq.rwth-aachen.de

import setuptools
import os

setuptools.setup(
    name="s3_smart_open",
    version=os.environ['VERSION'],
    author="Orakel",
    author_email="s.cramer@wzl.rwth-aachen.de",
    description="A package to save/upload/load/download to/from local or s3 storages",
    packages=setuptools.find_packages(),
    install_requires = [
        'smart_open==5.0.0',
        'boto3',
        'pandas',
        'pyyaml',
        'absl-py',
        'pyarrow',
        'joblib',
        'dill'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    license="License.txt"
)
