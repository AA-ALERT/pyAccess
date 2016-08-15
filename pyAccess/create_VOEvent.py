#!/usr/bin/env python

'''
description:    Create a db entry for a VOEvent
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

import argparse
import voeventparse as vp


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


def parse_VOEvent(voevent):
    '''
    parse VOEvent xml file
    '''
    # load VOEvent xml file
    v = vp.load(voevent)
    # assert if xml file is a valid VOEvent
    vp.assert_valid_as_v2_0(v)
    # load mapping VOEvent -> FRBCAT
    mapping = VOEvent_FRBCAT_mapping()
    # example
    newstr = 'v.' + (list(mapping.keys())[1])
    eval(newstr)  # evaluate newstr
    import pdb; pdb.set_trace()


def VOEvent_FRBCAT_mapping():
    '''
    Create a dictionary of dicts of VOEvent -> FRBCAT mapping
    Note: VOEvent=------- is not handled
    Q: do we need type?
    '''
    # TODO: define all mappings as defined in voeventfrbcatmatchings
    mapping = {
    'Why.Inference.Name': {'table':'frbs', 'column':'id', 'type':'INT UNSIGNED'},
    'WhereWhen.ObsDataLocation.ObservationLocation.AstroCoords.Time.TimeInstant.ISOTime': {'table':'frbs', 'column':'utc', 'type':'TIMESTAMP'},
    }
    #{'table':'', 'column':'', 'type':''}
    return mapping


if __name__=="__main__":
    voevents = cli_parser()
    for voevent in voevents:  # TODO: do we allow multiple xml files?
        parse_VOEvent(voevent)

