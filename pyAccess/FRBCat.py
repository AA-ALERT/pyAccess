'''
description:    FRBCat functionality for pyAccess
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import pymysql.cursors
import pandas as pd
from pyAccess import dbase as pAdbase
from pyAccess import utils as pAutils
import os
import sys

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
    values = [pAdbase.extract_from_db(
              cursor, event_id,
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
    convert={0:pAutils.strip, 1:pAutils.strip, 2:pAutils.strip, 3:pAutils.strip,
             4:pAutils.strip}
    # location of mapping.txt file
    mapping = os.path.join(os.path.dirname(sys.modules['pyAccess'].__file__),
                           'mapping.txt')
    df = pd.read_table(mapping, sep='/', engine='c', header=0,
                       skiprows=[0], skip_blank_lines=True,
                       skipinitialspace=True,
                       converters=convert).fillna('None')
    # todo: handle new events
    #mapping = Dictlist(zip(df.to_dict('split')['index'], df.to_dict('records')))
    #import pdb; pdb.set_trace()
    #return mapping
    # return pandas dataframe
    return df

