# Copyright (c) 2022 RWTH Aachen - Werkzeugmaschinenlabor (WZL)
# Contact: Simon Cramer, s.cramer@wzl-mq.rwth-aachen.de

import smart_open
import boto3
import os
import logging
import pickle
import dill
import botocore
import pandas as pd
import smart_open
import pyarrow
import json
import shutil
import joblib

logger = logging.getLogger(__name__)

def generate_s3_session():
    """Generates s3 Client or s3 Resource if needed
    Returns:
        boto3 Client and boto3 Resource
    """    
    s3c = boto3.client('s3',
                endpoint_url='https://'+os.environ['S3_ENDPOINT'],
                aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                )
    s3r = boto3.resource('s3',
                endpoint_url='https://'+os.environ['S3_ENDPOINT'],
                aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                )
    return s3c,s3r

def generate_s3_strings(path):
    """Generates s3 bucket name, s3 key and s3 path with an endpoint from a path
        with path (string): s3://BUCKETNAME/KEY
        x --> path.find(start) returns index 0 + len(start) returns 5 --> 0 + 5 = 5
        Y --> path[len(start):] = BUCKENAME/KEY --> .find(end) looking for forward slash in BUCKENAME/KEY --> returns 10
        Y --> now we have to add len(start) to 10 because the index was relating to BUCKENAME/KEY and not to s3://BUCKETNAME/KEY
        bucket_name = path[X:Y]
        Prefix is the string behind the slash that is behind the bucket_name
        - so path.find(bucket_name) find the index of the bucket_name, add len(bucket_name) to get the index to the end of the bucket name
        - add 1 because we do not want the slash in the Key

    Args:
        path (string): s3://BUCKETNAME/KEY
    Returns:
        strings:    path = s3://endpoint@BUCKETNAME/KEY
                    prefix = KEY
                    bucket_name = BUCKETNAME
    """    
    start = 's3://'
    end = '/'
    bucket_name = path[path.find(start)+len(start):path[len(start):].find(end)+len(start)]
    prefix = path[path.find(bucket_name)+len(bucket_name)+1:]
    if not prefix.endswith('/'):
        prefix =  prefix+'/'
    path = 's3://'+os.environ['S3_ENDPOINT']+'@'+bucket_name+'/'+prefix
    return bucket_name, prefix, path

def check_filenames(path,filenames_list,prefix,bucket_name):
    """Checks files from given filename_list, if they exists
    Args:
        path (str): Path for checking the filenames
        filenames_list (list[str]): List of filenames to check
        prefix (str): Name of the Key/Prefix to check
        bucket_name (str): Name of the bucket to check 
    Returns:
        [list]: List of filenames that do not exists in the given path
    """  
    if type(filenames_list)==str:
        filenames_list=[filenames_list]  
    filenames_remove = []
    if path[:5] == 's3://':
        for filename in filenames_list:
            try:
                s3c, s3r = generate_s3_session()
                s3r.Object(bucket_name, prefix+filename).load()
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    logger.warning("{}{}".format(filename,' does not exsist!'))
                    filenames_remove.append(filename)
    else:
        for filename in filenames_list:
            if not os.path.exists(os.path.join(path,filename)):
                logger.warning("{}{}".format(filename,' does not exsist!'))
                filenames_remove.append(filename)
    return filenames_remove

def get_filenames(input_path,filenames_list=None,file_types=None):
    """Looking for files in a given path. Checks files if a list of filenames is given
    Args:
        input_path (str): Path where to look for files
        filenames_list ([type]): Filenames to check if they exists
        file_types ([type]): File types to be filtered e.g. ".fth" , ".pckl"
    Returns:
        [list[str]]: List of filenames
    """    
    if type(filenames_list)==str:
        filenames_list=[filenames_list]
    filenames = []
    filenames_remove = []
    if input_path[:5] == 's3://':   
        bucket_name, prefix, input_path = generate_s3_strings(input_path)
        if filenames_list:
            filenames_remove = check_filenames(input_path,filenames_list,prefix,bucket_name)
            filenames = filenames_list
        else: #Looking for Keys in the given Bucket that include the prefix
            s3c, s3r = generate_s3_session()
            paginator = s3c.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name,Prefix=prefix,)
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        filename = obj['Key'][obj['Key'].rfind('/')+1:]
                        if not filename == '':
                            filenames.append(filename)
                else:
                    logger.info('No Files in s3 were found!')
    else:   
        if filenames_list:
            filenames_remove = check_filenames(input_path,filenames_list,None,None)
            filenames = filenames_list
        else:
            for root, dirs, files in os.walk(input_path):
                for file in files:
                        filenames.append(file)
    if file_types:
        for filename in filenames:
            if  filename[filename.rfind('.'):] not in file_types:
                filenames_remove.append(filename)
    for filename in filenames_remove:
        filenames.remove(filename)
    logger.info('{}{}'.format('Returning filenames: ',filenames))
    return filenames
 
def _get_file_handle(path,filename):
    """Editing the path for read and write functions
    Args:
        path (str): Path to the file for reading or path for writing files
        filename (list[str]): file that will be read or will be written to the path
    Returns:
        [str]: Full path that includes the filename and a correct syntax for reading and writing functions
    """    
    if path[:5] == 's3://':
        if path.endswith('/') or filename.startswith('/'):
            path = 's3://'+os.environ['S3_ENDPOINT']+'@'+path[5:]+filename
        else:
            path = 's3://'+os.environ['S3_ENDPOINT']+'@'+path[5:]+'/'+filename
    else:
        os.makedirs(path, exist_ok=True)
        path = os.path.join(path,filename)
    return path

def read_pd_fth(input_path,filename,columns=None,col_limit=None):
    """Reads feather file from path and returns a pandas dataframe
    Args:
        input_path (str): Path to the file to read
        filename (str): Filename of the file to read
        columns (list[str]): If not provided, all columns are read.
        col_limit (int): Use col_limit to check if the amount of columns is not higher than col_limit!
    Returns:
        [pandas.DataFrame]: Pandas Dataframe
    """    
    if not get_filenames(input_path,filenames_list=filename):
        raise ValueError('Input path or filename does not exist!')
    if columns and type(columns) != list:
        columns = [columns]
    savepath = _get_file_handle(input_path,filename)
    logger.info('----Download started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'rb', transport_params={'client': s3c}) as infile: 
        pandas_file =  pd.read_feather(infile,columns)
    if 'index' in pandas_file.columns:
        pandas_file.drop(columns=['index'], inplace=True)
        logger.info('Dropped column index on import from file {}. If this is unintentional rename column in file.'.format(filename))
    logger.info('----Download finished: {} ----'.format(filename))
    pandas_file.reset_index()
    if col_limit:
        assert pandas_file.shape[1] <= int(col_limit), 'Amount of columns is higher than the provided limit of columns. Use col_limit when only a specified amount of columns is allowed for further execution!'
    return pandas_file

def read_pckl(input_path,filename):
    """Reads pickle file from path and returns the pickled object
    Args:
        input_path (str): Path to the file to read
        filename (str): Filename of the file to read
    Returns:
        Can be everything that is pickleable
    """     
    if not get_filenames(input_path,filenames_list=filename):
        raise ValueError('Input path or filename does not exist!')
    savepath = _get_file_handle(input_path,filename)
    logger.info('----Download started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'rb', transport_params={'client': s3c}) as infile: 
        pickle_file =  pickle.load(infile)
    logger.info('----Download finished: {} ----'.format(filename))
    return pickle_file

def read_dill(input_path,filename):
    """Reads dill file from path and returns the serialized object
    Args:
        input_path (str): Path to the file to read
        filename (str): Filename of the file to read
    Returns:
        Can be everything that can be serialized with dill
    """     
    if not get_filenames(input_path,filenames_list=filename):
        raise ValueError('Input path or filename does not exist!')
    savepath = _get_file_handle(input_path,filename)
    logger.info('----Download started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'rb', transport_params={'client': s3c}) as infile: 
        dill_file =  dill.load(infile)
    logger.info('----Download finished: {} ----'.format(filename))
    return dill_file

def read_joblib(input_path, filename):
    """Reads joblib file from path and returns the joblib object
    Args:
        input_path (str): Path to the file to read
        filename (str): Filename of the file to read
    Returns:
        Can be everything that is pickleable.
    """ 
    if not get_filenames(input_path,filenames_list=filename):
        raise ValueError('Input path or filename does not exist!')

    savepath = _get_file_handle(input_path,filename)
    logger.info('----Download started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()

    with smart_open.open(savepath,'rb', transport_params={'client': s3c}) as infile: 
        joblib_file =  joblib.load(infile)

    logger.info('----Download finished: {} ----'.format(filename))

    return joblib_file

def to_pd_fth(output_path,filename,dataframe):
    """Writes a pandas Dataframe to a given path as feather file.
    Args:
        output_path (str): Path to the file to write.
        filename (str): Filename of the file to write.
        dataframe (pandas.Datarame): Pandas Dataframe which should be saved as .fth.
    """    
    savepath = _get_file_handle(output_path,filename)
    logger.info('----Upload started: {} ----'.format(filename))
    dataframe.reset_index()
    arrow_table = pyarrow.Table.from_pandas(dataframe,preserve_index=False)
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'wb', transport_params={'client': s3c}) as outfile: 
        pyarrow.feather.write_feather(arrow_table,outfile)
    logger.info('----Upload finished: {} ----'.format(filename))

def to_pckl(output_path,filename,data):
    """Writes an object to a given path as pickle file.
    Args:
        output_path (str): Path to the file to write.
        filename (str): Filename of the file to write.
        data (anything): Could be anything that is pickleable.
    """      
    savepath = _get_file_handle(output_path,filename)
    logger.info('----Upload started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'wb', transport_params={'client': s3c}) as outfile: 
        pickle.dump(data,outfile)
    logger.info('----Upload finished: {} ----'.format(filename))

def to_dill(output_path,filename,data):
    """Writes an object to a given path as dill file.
    Args:
        output_path (str): Path to the file to write.
        filename (str): Filename of the file to write.
        data (anything): Could be anything that can be serialized with dill.
    """      
    savepath = _get_file_handle(output_path,filename)
    logger.info('----Upload started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'wb', transport_params={'client': s3c}) as outfile: 
        dill.dump(data,outfile)
    logger.info('----Upload finished: {} ----'.format(filename))

def to_joblib(output_path, filename, data):
    """Writes an object to a given path as joblib file.
    Args:
        output_path (str): Path to the file to write.
        filename (str): Filename of the file to write.
        data (anything): Could be anything that is pickleable.
    """      
    savepath = _get_file_handle(output_path,filename)
    logger.info('----Upload started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()

    with smart_open.open(savepath,'wb', transport_params={'client': s3c}) as outfile: 
        joblib.dump(data, outfile)
        
    logger.info('----Upload finished: {} ----'.format(filename))


def to_s3(output_path,filename,data):
    """Uploads a given object to s3 storage.
    Args:
        output_path (str): Path to s3 storage
        filename (str): The object will be save with this filename
        data (anything): Object that will be uploaded to the s3 storage
    """    
    bucket_name, prefix, path = generate_s3_strings(output_path)
    s3c, s3r = generate_s3_session()
    try:
        response = s3c.upload_file(data, bucket_name, prefix+filename)
    except botocore.exceptions.ClientError as e:
        logger.error(e)
    
def to_json(output_path,filename,data):
    """Uploads a given object as json file to s3 storage
    Args:
        output_path (str): Path to s3 storage
        filename (str): The object will be save with this filename
        data (anything): Object that will be uploaded to the s3 storage
    """    
    savepath = _get_file_handle(output_path,filename)
    logger.info('----Upload started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'w', transport_params={'client': s3c}) as outfile: 
        json.dump(data,outfile)
    logger.info('----Upload finished: {} ----'.format(filename))

def read_json(input_path,filename):
    """Reads json file from path and returns json content
    Args:
        input_path (str): Path to the file to read
        filename (str): Filename of the file to read
    Returns:
        Json content
    """     
    if not get_filenames(input_path,filenames_list=filename):
        raise ValueError('Input path or filename does not exist!')
    savepath = _get_file_handle(input_path,filename)
    logger.info('----Download started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'r', transport_params={'client': s3c}) as infile: 
        json_file =  json.load(infile)
    logger.info('----Download finished: {} ----'.format(filename))
    return json_file

def to_txt(output_path,filename,data):
    """Uploads a string as txt file to s3 storagee
    Args:
        output_path (str): Path to s3 storage
        filename (str): The string will be save with this filename
        data (str): string that will be uploaded to the s3 storage
    """    
    savepath = _get_file_handle(output_path,filename)
    logger.info('----Upload started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'w', transport_params={'client': s3c}) as outfile: 
        outfile.write(data)
    logger.info('----Upload finished: {} ----'.format(filename))

def read_txt(input_path, filename):
    """Reads a txt file.
    Args:
        input_path (str): Path to the file to read
        filename (str): Filename of the file to read
    Raises:
        ValueError: When file or filepath do not exsist
    Returns:
        [str]: txt file content
    """    
    if not get_filenames(input_path,filenames_list=filename):
        raise ValueError('Input path or filename does not exist!')
    savepath = _get_file_handle(input_path,filename)
    logger.info('----Download started: {} ----'.format(filename))
    s3c, s3r = generate_s3_session()
    with smart_open.open(savepath,'r', transport_params={'client': s3c}) as infile: 
        txt_file =  infile.read()
    logger.info('----Download finished: {} ----'.format(filename))
    return txt_file

def from_s3(input_path,filename,output_path=None):
    """Downloads a given object from s3 storage to local disk
    Args:
        input_path (str): Path to s3 storage, ! without filename !
        filename (str): Name of the Object in the s3 storage.
        output_path (str): Path where object will be saved on local disk
    """    
    bucket_name, prefix, path = generate_s3_strings(input_path)
    s3c, s3r = generate_s3_session()
    try:
        if output_path == None : 
             s3c.download_file(bucket_name, prefix+filename,os.path.join(filename))
        else: 
            s3c.download_file(bucket_name, prefix+filename,os.path.join(output_path,filename))
    except botocore.exceptions.ClientError as e:
        logger.error(e)

def local_directory_to_s3(input_path, output_path, directory_name):
    """Upload a local directory to a s3 bucket.

    Args:
        input_path (str): Local path to the directory. Does not include the directory to upload itself.
        output_path (str): Path the to the s3 bucket+prefix where the directory should be uploaded to
        directory_name (str): name of the directory to upload.
    """    
    bucket_name, prefix, path = generate_s3_strings(output_path)
    s3c, s3r = generate_s3_session()

    local_directory = os.path.join(input_path, directory_name)

    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(directory_name, relative_path)

            try:
                s3c.upload_file(local_path, bucket_name, prefix + s3_path)
            except botocore.exceptions.ClientError as e:
                logger.error(e)

def delete_s3_objects(path,filenames=None,file_types=None):
    """Delete a folder or objects inside a folder/bucket+key(s3) and optional in combination with given filenames and file types.

    Args:
        path (str): s3 path (s3://bucketname/key)
        filenames=None (list[str]): List of filenames that will be delete. When no filenames are given all files inside the the folder/key will be deleted.
        file_types=None (list[str]): When file type is not none only files with the given file types will be deleted.
    """        
    if path[:5] == 's3://':
        sub_prefix = []
        s3c, s3r = generate_s3_session()
        bucket_name, main_prefix, tmp_path = generate_s3_strings(path)
        paginator = s3c.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name,Prefix=main_prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    prefix = obj['Key'][:obj['Key'].rfind('/')+1]
                    if not prefix == '' and prefix not in sub_prefix:
                        sub_prefix.append(prefix)
        for sub_p in sub_prefix:
            sub_path  = 's3://' + bucket_name + '/' + sub_p
            filenames_to_delete = get_filenames(sub_path,filenames,file_types)
            for f in filenames_to_delete:
                try:
                    s3r.Object(bucket_name, sub_p + f ).delete()
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logger.warning("Failed to delete : {} ; Reason {}".format(f,e))
    elif os.path.exists(path):
        if filenames == None:
            try:
                shutil.rmtree(path)
            except Exception as e:
                logger.warning("Failed to delete : {} ; Reason {}".format(f,e))
        else:
            filenames = get_filenames(path,filenames,file_types)
            for f in filenames:
                try:
                    os.remove(os.path.join(path,f))
                except Exception as e:
                    logger.warning("Failed to delete : {} ; Reason {}".format(f,e))
