# encoding=utf-8
from cootek_desensitization import *

def encode_dict(d):
    for key in ['identifier', 'mac_address', 'imei']:
        if key in d and d[key] and d[key] != 'none':
            if len(d[key]) == 32 and '#' not in d[key] and key=='identifier':
                raise Exception('key: %s - %s has been encoded' % (key, d[key]))
            d[key] = desensitization(d[key])
    return d

