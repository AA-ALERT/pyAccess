'''
description:    Decode VOEvent db entry to xml
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import argparse
import voeventparse as vp
import pandas
from pyAccess import utils
from pyAccess import dbase
from pyAccess.FRBCat import *

def decode_FRBCat_entry():
    '''
    Decode FRBCat entry
    '''
    # load mapping VOEvent -> FRBCat
    mapping = VOEvent_FRBCAT_mapping()
    # connect to database
    # TODO: add connection details
    connection, cursor = dbase.connectToDB(dbName='frbcat',
                                           userName='aa-alert',
                                           dbPassword='aa-alert')
    FRBCat = FRBCat_decode(connection, cursor, mapping, 1)
    FRBCat.decode_VOEvent_from_FRBCat()
