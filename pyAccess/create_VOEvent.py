'''
description:    Create a db entry for a VOEvent
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import argparse
import voeventparse as vp
import pandas
from pyAccess import dbase
from pytz import timezone
from pyAccess.FRBCat import *
from pyAccess import utils


def get_param(param_data, mapping, idx):
    '''
    Get param data for a given attribute
    '''
    mapping['VOEvent TYPE'][idx] not in ['Param', 'Coord', 'ISOTime']
    try:
        return (param_data[mapping['VOEvent'].iloc[idx]]
                [mapping['FRBCAT COLUMN'].iloc[idx]])
    except KeyError:
        return None


def get_coord(v, mapping, idx):
    '''
    Get astro coordinates
    '''
    switcher = {
        'raj': 'ra',
        'decj': 'dec',
        'gl': 'gl',
        'gb': 'gb',
        'pointing_error': 'err',
    }
    try:
        utils.decdeg2dms(vp.pull_astro_coords(v, index=0).ra)
        key = switcher[[mapping['FRBCAT COLUMN'].iloc[idx]][0]]
        if key in ['raj', 'decj', 'ra', 'dec']:
            return utils.decdeg2dms(getattr(vp.pull_astro_coords(v, index=0),
                                    key))
        else:
            return getattr(vp.pull_astro_coords(v, index=0),
                           key)
    except AttributeError:
        return None
    except KeyError:
        return None


def get_attrib(v, mapping, idx):
    '''
    Get xml attributes
    '''
    try:
        return v.attrib[mapping['VOEvent'].iloc[idx]]
    except ValueError:
        return None
    except KeyError:
        return None


def get_utc_time_str(v):
    '''
    Get time in UTC
    Return string 'YYYY-MM-DD HH:MM:SS'
    '''
    isotime = vp.pull_isotime(v, index=0)
    # convert to UTC
    utctime = isotime.astimezone(timezone('UTC'))
    # return time in UTC string
    return utctime.strftime("%Y-%m-%d %H:%M:%S")


def get_value(v, param_data, mapping, idx):
    switcher = {
        'Param':    get_param(param_data, mapping, idx),
        'Coord':    get_coord(v, mapping, idx),
        'ISOTime':  get_utc_time_str(v),
        'XML':      vp.dumps(v),
        'attrib':   get_attrib(v, mapping, idx),
        '':         None
    }
    # get function from switcher dictionary
    return switcher[mapping['VOEvent TYPE'].iloc[idx]]


def parse_VOEvent(voevent, mapping):
    '''
    parse VOEvent xml file
        input:
        - voevent: VOEvent xml file
        - mapping: mapping dictionary voevent -> FRBCAT
        returns vo_dict, mapping
        - vo_dict: dictionary vo_event: value
        - mapping: dictionary vo_event: FRBCAT location
    '''
    # load VOEvent xml file
    v = vp.load(voevent)
    # assert if xml file is a valid VOEvent
    vp.assert_valid_as_v2_0(v)
    # Check if the event is a new VOEvent
    # For a new VOEvent there should be no citations
    if not v.xpath('Citations'):
        # new VOEvent
        mapping = VOEvent_FRBCAT_mapping(new_event=True)
    else:
        mapping = VOEvent_FRBCAT_mapping(new_event=False)
    # use the mapping to get required data from VOEvent xml
    # if a path is not found in the xml it gets an empty list which is
    # removed in the next step
    # puts all params into dict param_data[group][param_name]
    param_data = vp.pull_params(v)
    vo_data = (lambda v=v, mapping=mapping: (
               [v.xpath('.//' + event.replace('.', '/')) if mapping[
                'VOEvent TYPE'].iloc[idx] not in [
                'Param', 'Coord', 'ISOTime', 'XML', 'attrib']
                and event else get_value(
                v, param_data, mapping, idx) for idx, event in
                enumerate(mapping['VOEvent'])]))()
    vo_data = [None if not a else a for a in vo_data]
    vo_alta = (lambda v=v, mapping=mapping: (
               [v.xpath('.//' + event.replace('.', '/')) if mapping[
                'VOEvent TYPE'].iloc[idx] not in [
                'Param', 'Coord', 'ISOTime', 'XML', 'attrib']
                and event else get_value(
                v, param_data, mapping, idx) for idx, event in
                enumerate(mapping['VOEvent_alt'])]))()
    vo_alta = [None if not a else a for a in vo_alta]
    # TODO: merging is a placeholder:
    # some things may depend on new/not new event
    merged = (lambda vo_data=vo_data, vo_alta=vo_alta: ([vo_data[idx] if
              vo_data[idx] else vo_alta[idx] for idx in
              range(0, len(vo_alta))]))()
    # make sure we don't have any lists here
    merged = [x[0] if isinstance(x, list) else x for x in merged]
    # add to pandas dataframe as a new column
    mapping.loc[:, 'value'] = pandas.Series(merged, index=mapping.index)
    # need to add xml file to database as well
    return mapping


def process_VOEvent(voevent):
    '''
    process VOEvent
        input:
        - voevent: VOEevent xml file
    '''
    # load mapping VOEvent -> FRBCAT
    mapping = VOEvent_FRBCAT_mapping()
    # parse VOEvent xml file
    vo_dict = parse_VOEvent(voevent, mapping)
    # create a new FRBCat entry  # TODO: handle other types
    new_FRBCat_entry(vo_dict)


def new_FRBCat_entry(mapping):
    '''
    Add new FRBCat entry
    '''
    # connect to database
    # TODO: add connection details
    connection, cursor = dbase.connectToDB(dbName='frbcat',
                                           userName='aa-alert',
                                           dbPassword='aa-alert')
    FRBCat = FRBCat_add(connection, cursor, mapping)
    FRBCat.add_VOEvent_to_FRBCat()
