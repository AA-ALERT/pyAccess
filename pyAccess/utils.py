#!/usr/bin/env python

'''
description:    Common functionality for pyAccess
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import pymysql.cursors
import logging
from logging.handlers import RotatingFileHandler

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


def connectToDB(dbName = None, userName= None, dbPassword = None, dbHost = None,
                dbPort = None):
    '''
    Connect to a specified MySQL DB and return connection and cursor objects.
    '''
    # Start DB connection
    try: 
        connection = pymysql.connect(host=dbHost,
                                     port=dbPort,
                                     user=userName,
                                     password=dbPassword,
                                     db=dbName,
                                     cursorclass=pymysql.cursors.DictCursor)
    except Exception as E:
        err_msg = 'Unable to connect to %s DB.'% dbName
        logging.error((err_msg, "; %s: %s" % (E.__class__.__name__, E)))
        raise
    msg = 'Successful connected to %s DB.'%dbName
    logging.debug(msg)
    # if the connection succeeded get a cursor    
    cursor = connection.cursor()
return connection, cursor


def closeDBConnection(connection, cursor):
    '''
    Closes a connection to a DB given the connection and cursor objects
    '''
    cursor.close()
    connection.close()    
    msg = 'Connection to the DB is closed.'
    logging.debug(msg)
return
