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
from numpy import append as npappend
from numpy import array as nparray
from pyAccess import dbase
from numpy import where as npwhere
from numpy import ravel as npravel

class FRBCat_add:
    def __init__(self, connection, cursor, mapping):
        self.connection = connection
        self.cursor = cursor
        self.mapping = mapping

    def author_exists(self, ivorn):
        '''
        Check if author already exists in database
        if author is found, set self.author_id
        '''
        # check if the author ivorn is already in the database
        author_id = dbase.extract_from_db_sql(self.cursor, 'authors', 'id', 'ivorn', ivorn)
        if not author_id:  # did not find the author ivorn
            return False
        else:  # set self.author_id to the one in the database
            self.author_id = author_id['id']
            return True
 
    def add_authors(self, table, rows, value):
        '''
        Add author to the database if the ivorn is not in the authors table
        '''
        # check if author already exists in database
        # TODO: try/except
        ivorn = value[npwhere(rows == 'ivorn')][0]
        author_exists = self.author_exists(ivorn)
        # add author to database if author does not yet exist in db
        if not author_exists:
            self.author_id = self.insert_into_database(table, rows, value)

    def add_frbs(self, table, rows, value):
        '''
        Add event to the frbs table
        '''
        rows = npappend(rows, 'author_id')
        value = npappend(value, self.author_id)
        self.frb_id = self.insert_into_database(table, rows, value)

    def add_frbs_notes(self, table, rows, value):
        '''
        Add event to the frbs_notes table
        '''
        rows = npappend(rows, ('frb_id'))
        value = npappend(value, (self.frb_id))
        frb_notes_id = self.insert_into_database(table, rows, value)

    def add_frbs_have_publications(self, table, rows, value):
        '''
        Add event to the frbs_have_publications table
        '''
        rows = npappend(rows, ('frb_id', 'pub_id'))
        value = npappend(value, (self.frb_id, self.pub_id))
        self.insert_into_database(table, rows, value)
    
    def add_observations(self, table, rows, value):
        '''
        Add event to the observations table
        '''
        rows = npappend(rows, ('frb_id', 'author_id')) 
        value = npappend(value, (self.frb_id, self.author_id))
        self.obs_id = self.insert_into_database(table, rows, value)

    def add_observations_notes(self, table, rows, value):
        '''
        Add event to the observations_notes table
        '''
        rows = npappend(rows, ('obs_id')) 
        value = npappend(value, (self.obs_id))
        obs_notes_id = self.insert_into_database(table, rows, value)

    def add_observations_have_publications(self, table, rows, value):
        '''
        Add event to the observations_have_publications table
        '''
        rows = npappend(rows, ('obs_id', 'pub_id'))
        value = npappend(value, (self.obs_id, self.pub_id))
        self.insert_into_database(table, rows, value)

    def add_radio_observations_params(self, table, rows, value):
        '''
        Add event to the radio_observations_params table
        '''
        rows = npappend(rows, ('obs_id', 'author_id'))
        value = npappend(value, (self.obs_id, self.author_id))
        self.rop_id = self.insert_into_database(table, rows, value)

    def add_radio_observations_params_notes(self, table, rows, value):
        '''
        Add event to the radio_observations_params_notes table
        '''
        rows = npappend(rows, ('rop_id'))
        value = npappend(value, (self.rop_id))
        rop_notes_id = self.insert_into_database(table, rows, value)

    def add_radio_observations_params_have_publications(self, table, rows, value):
        '''
        Add event to the radio_observations_params_have_publications table
        '''
        rows = npappend(rows, ('rop_id', 'pub_id'))
        value = npappend(value, (self.rop_id, self.pub_id))
        self.insert_into_database(table, rows, value)

    def add_radio_measured_params(self, table, rows, value):
        '''
        Add event to the radio_measured_params table
        '''
        rows = npappend(rows, ('rop_id', 'author_id'))
        value = npappend(value, (self.rop_id, self.author_id))
        self.rmp_id = self.insert_into_database(table, rows, value)

    def add_radio_measured_params_notes(self, table, rows, value):
        '''
        Add event to the radio_measured_params_notes table
        '''
        rows = npappend(rows, ('rmp_id'))
        value = npappend(value, (self.rmp_id))
        rmp_notes_id = self.insert_into_database(table, rows, value)

    def add_radio_measured_params_have_publications(self, table, rows, value):
        '''
        Add event to the radio_measured_params_have_publications table
        '''
        rows = npappend(rows, ('rmp_id', 'pub_id'))
        value = npappend(value, (self.rmp_id, self.pub_id))
        self.insert_into_database(table, rows, value)

    def add_publications(self, table, rows, value):
        '''
        Add event to the publications table
        '''
        self.pubid = self.insert_into_database(table, rows, value)

    def add_radio_images(self, table, rows, value):
        '''
        Add event to the radio_images table
        '''
        self.rid = self.insert_into_database(table, rows, value)

    def add_radio_images_have_rmp(self, table, rows, value):
        '''
        Add event to the radio_images_have_rmp table
        '''
        rows = npappend(rows, ('radio_image_id', 'rmp_id'))
        value = npappend(value, (self.rid, self.rmp_id))    
        self.insert_into_database(table, rows, value)

    def insert_into_database(self, table, rows, value):
        row_sql = ', '.join(map(str, rows))
        self.cursor.execute("INSERT INTO {} ({}) VALUES {}".format(table,row_sql,tuple(value)))
        return self.connection.insert_id()  # alternatively cursor.lastrowid


    def add_VOEvent_to_FRBCat(self):
        '''
        Add a VOEvent to the FRBCat database
          - input:
              connection: database connection
              cursor: database cursor object
              mapping: mapping between database entry and VOEvent extracted value
                         db tables in mapping['FRBCAT TABLE']
                         db columns in mapping['FRBCAT COLUMN']
                         db values in mapping['values']
        '''
		# get FRBCat db tables from pandas dataframe mapping
		#tables = set(mapping['FRBCAT TABLE'].values)
        tables = ['authors', 'frbs', 'frbs_notes', 'observations', 'observations_notes',
                  'radio_observations_params', 'radio_observations_params_notes',
                  'radio_measured_params', 'radio_measured_params_notes']
		# loop over defined tables
        for table in tables:
            try:
                del value
            except NameError:
                pass
	        # extract the rows from the mapping that are in the table
            to_add = self.mapping.loc[(self.mapping['FRBCAT TABLE'] == table) &
                                      (self.mapping['value'].notnull())]
            # extract db rows and values to add
            rows = to_add['FRBCAT COLUMN'].values
   	        # loop over extracted rows and insert values
            for row in rows:
                # extract value from pandas dataframe
                try:
                    value
                    value.append(to_add.loc[
                                 to_add[
                                 'FRBCAT COLUMN'] == row]['value'].values[0])
                except UnboundLocalError:
                    value = [to_add.loc[to_add[
                             'FRBCAT COLUMN'] == row]['value'].values[0]]
            value = npravel(nparray(value))  # convert to numpy array
            if table == 'authors':
                self.add_authors(table, rows, value)
            if table == 'frbs':
                self.add_frbs(table, rows, value)
            if table == 'frbs_notes':
                self.add_frbs_notes(table, rows, value)
            if table == 'observations':
                self.add_observations(table, rows, value)
            if table == 'observations_notes':
                self.add_observations_notes(table, rows, value)
            if table == 'radio_observations_params':
                self.add_radio_observations_params(table, rows, value)
            if table == 'radio_observations_params_notes':
                self.add_radio_observations_params_notes(table, rows, value)
            if table == 'radio_measured_params':
                self.add_radio_measured_params(table, rows, value)
            if table == 'radio_measured_params_notes':
                self.add_radio_measured_params_notes(table, rows, value)
		   #     # try to insert
		   #     try:
		   #        cursor.execute("INSERT INTO ({}) VALUES (())".format(row,value))
		   #        connection.commit()
		   #     except:
		   #        connection.rollback()

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
    #return mapping
    # return pandas dataframe
    return df

