#!/usr/bin/env python

'''
description:    Decode VOEvent db entry to xml
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import argparse
import voeventparse as vp
import pandas
import utils
import dbase

def decode_FRBCat_entry():
    '''
    Decode FRBCat entry
    '''
    # connect to database
    connection, cursor = dbase.connectToDB()  # TODO: add connection details
    # load mapping VOEvent -> FRBCat
    mapping = utils.VOEvent_FRBCAT_mapping()
    # 
    utils.decode_VOEvent_from_FRBCat(cursor, mapping)


if __name__=="__main__":
    decode_FRBCat_entry()
