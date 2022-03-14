# Copyright (c) 2022 RWTH Aachen - Werkzeugmaschinenlabor (WZL)
# Contact: Simon Cramer, s.cramer@wzl-mq.rwth-aachen.de

import yaml
import os
import logging


logger = logging.getLogger(__name__)


config_path=os.path.join('s3config','config.yaml')

if os.path.exists(config_path):
    with open(config_path,'r') as stream:
        config = yaml.safe_load(stream)
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']
    os.environ['S3_ENDPOINT'] = config['S3_ENDPOINT']
else:
    if not ('AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' and 'S3_ENDPOINT') in os.environ:
        logger.warning('S3 connection could not be configured. No config file was found. Filehandler will only work for local files!')
        os.environ['AWS_ACCESS_KEY_ID'] = ""
        os.environ['AWS_SECRET_ACCESS_KEY'] = ""
        os.environ['S3_ENDPOINT'] = "s3"

from .filehandler import *
