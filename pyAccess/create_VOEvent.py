#!/usr/bin/env python

'''
description:    Create a db entry for a VOEvent
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import argparse
import voeventparse as vp
import utils
import pandas


def cli_parser():
    '''
    parse command line arguments:
        should be single/multiple valid VOEvent xml files
    '''
    parser = argparse.ArgumentParser(description='Process VOEvent XML file '
                                     'and add it to FRB database')
    parser.add_argument('VOEvents', metavar='VOEvent',
                        type=argparse.FileType('rb'), nargs='+',
                        help='List of VOEvent XML files')
    results = vars(parser.parse_args())['VOEvents']
    return results


def get_param(param_data, mapping, idx):
    '''
    Get param data for a given attribute
    '''
    mapping['VOEvent TYPE'][idx] not in ['Param', 'Coord', 'ISOTime']
    try:
        return param_data[mapping['VOEvent'].iloc[idx]][mapping['FRBCAT COLUMN'].iloc[idx]]
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
        'pointing_error': 'Error2Radius',
    }
    try:
        return getattr(vp.pull_astro_coords(v, index=0),
                       switcher[[mapping['FRBCAT COLUMN'].iloc[idx]][0]])
    except AttributeError:
        return None
    except KeyError:
        return None


def get_value(v, param_data, mapping, idx):
    switcher = {
        'Param':    get_param(param_data, mapping, idx),
        'Coord':    get_coord(v, mapping, idx),
        'ISOTime':  vp.pull_isotime(v, index=0),
        'XML':      vp.dumps(v),
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
        mapping = utils.VOEvent_FRBCAT_mapping(new_event=True)
    else:
        mapping = utils.VOEvent_FRBCAT_mapping(new_event=False)
    # use the mapping to get required data from VOEvent xml
    # if a path is not found in the xml it gets an empty list which is
    # removed in the next step
    param_data = vp.pull_params(v)  # puts all params into dict param_data[group][param_name]
    vo_data = (lambda v=v,mapping=mapping: (
               [v.xpath('.//' + event.replace('.','/')) if mapping[
               'VOEvent TYPE'].iloc[idx] not in ['Param', 'Coord', 'ISOTime', 'XML']
               and event else get_value(
               v, param_data, mapping, idx) for idx,event in
               enumerate(mapping['VOEvent'])]))()
    # add to pandas dataframe as a new column
    mapping.loc[:,'value'] = pandas.Series(vo_data, index=mapping.index)
    # need to add xml file to database as well
    import pdb; pdb.set_trace()
    return mapping


def process_VOEvent(voevent):
    '''
    process VOEvent
        input:
        - voevent: VOEevent xml file
    '''
    # load mapping VOEvent -> FRBCAT
    mapping = utils.VOEvent_FRBCAT_mapping()
    # parse VOEvent xml file
    vo_dict = parse_VOEvent(voevent, mapping)
    # create a new FRBCat entry  # TODO: handle other types
    new_FRBCat_entry(vo_dict)


def new_FRBCat_entry(mapping):
    '''
    Add new FRBCat entry
    '''
    # connect to database
    connection, cursor = utils.connectToDB()  # TODO: add connection details
    # 
    utils.add_VOEvent_to_FRBCat(cursor, mapping)


if __name__=="__main__":
    voevents = cli_parser()
    for voevent in voevents:  # TODO: do we allow multiple xml files?
        vo_data, mapping = process_VOEvent(voevent)

