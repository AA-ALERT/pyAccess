#!/usr/bin/env python

'''
description:    Common functionality for pyAccess
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import pymysql.cursors
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from pyAccess import dbase


# define global LOG variables
DEFAULT_LOG_LEVEL = 'debug'
DEFAULT_LOG_LEVEL_C = 'warning'
LOG_LEVELS = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'warning': logging.WARNING,
              'error': logging.ERROR,
              'critical': logging.CRITICAL}
LOG_LEVELS_LIST = LOG_LEVELS.keys()
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = "%Y/%m/%d/%H:%M:%S"

logger = None


def start_logging(filename, level=DEFAULT_LOG_LEVEL,
                  level_c=DEFAULT_LOG_LEVEL_C):
    '''
    Start logging with given filename and level.
        filename: logfile filename
        level: minumum log level written to logfile
        level_c: minimum log level written to std_err
    '''
    global logger
    if logger == None:
        logger = logging.getLogger()
    else:  # wish there was a logger.close()
        for handler in logger.handlers[:]:  # make a copy of the list
            logger.removeHandler(handler)
    logger.setLevel(LOG_LEVELS[level])
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    # Define and add file handler
    fh = RotatingFileHandler(filename,
                             maxBytes=(10*1024*1024),
                             backupCount=7)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # Define a handler which writes messages level_c or higher to std_err
    console = logging.StreamHandler()
    console.setLevel(LOG_LEVELS[level_c])
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logger.addHandler(console)
    return logger


def add_VOEvent_to_FRBCat(cursor, mapping):
    '''
    Add a VOEvent to the FRBCat database
      - input:
          cursor: database cursor object
          mapping: mapping between database entry and VOEvent extracted value
                     db tables in mapping['FRBCAT TABLE']
                     db columns in mapping['FRBCAT COLUMN']
                     db values in mapping['values']
    '''
    # get FRBCat db tables from pandas dataframe mapping
    tables = set(mapping['FRBCAT TABLE'].values)
    # loop over defined tables
    for table in tables:
        # extract the rows from the mapping that are in the table
        to_add = mapping.loc[(mapping['FRBCAT TABLE'] == table) &
                             (mapping['value'].notnull())]
        # extract db rows and values to add
        rows = to_add['FRBCAT COLUMN'].values
        # loop over extracted rows and insert values
        for row in rows:
            # extract value from pandas dataframe
            value = to_add.loc[to_add[
                               'FRBCAT COLUMN'] == row]['value'].values[0]
            # try to insert
            try:
               cursor.execute("INSERT INTO ({}) VALUES (())".format(row,value))
               db.commit()
            except:
               db.rollback()


def decode_VOEvent_from_FRBCat(cursor, mapping, event_id):
    '''
    Decode a VOEvent from the FRBCat database
      input:
        cursor: database cursor object
        mapping: mapping between database entry and VOEvent xml
      output:
        updated mapping with added values column
    '''
    # extract values from db for each row in mapping pandas dataframe
    values = [dbase.extract_from_db(cursor, event_id,
                                    mapping.iloc[idx]['FRBCAT TABLE'],
                                    mapping.iloc[idx]['FRBCAT COLUMN']) for idx,
                                    row in mapping.iterrows()]
    # add to pandas dataframe as a new column
    mapping.loc[:,'value'] = pandas.Series(values, index=mapping.index)
    return mapping


def VOEvent_FRBCAT_mapping(new_event=True):
    '''
    Create a dictionary of dicts of VOEvent -> FRBCAT mapping
        new_event: boolean indicating if event is a new event,default=True
    '''
    # read mapping.txt into a pandas dataframe
    convert={0:strip, 1:strip, 2:strip, 3:strip, 4:strip}
    df = pd.read_table('mapping.txt', sep='/', engine='c', header=0,
                       skiprows=[0], skip_blank_lines=True,
                       skipinitialspace=True,
                       converters=convert).fillna('None')
    # todo: handle new events
    #mapping = Dictlist(zip(df.to_dict('split')['index'], df.to_dict('records')))
    #import pdb; pdb.set_trace()
    #return mapping
    # return pandas dataframe
    return df


def strip(text):
    try:
        return text.strip()
    except AttributeError:
        return text


class Dictlist(dict):
    def __setitem__(self, key, value):
        try:
            self[key]
        except KeyError:
            super(Dictlist, self).__setitem__(key, [])
        self[key].append(value)
