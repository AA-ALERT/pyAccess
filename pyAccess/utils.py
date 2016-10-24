'''
description:    Common functionality for pyAccess
license:        APACHE 2.0
author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
'''

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

def decdeg2dms(dd):
   # check if positive
   is_positive = dd >= 0
   dd = abs(dd)  # absolute value
   minutes,seconds = divmod(dd*3600,60)
   degrees,minutes = divmod(minutes,60)
   degrees = degrees if is_positive else -degrees
   return (str(int(degrees))+':'+str(int(minutes))+':'+str(int(seconds)))
