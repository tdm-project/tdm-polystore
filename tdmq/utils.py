import re

def convert_footprint(footprint):
    # Circle
    fnum = '([-+]?\d+(\.\d*)?)'
    circle_re = 'circle\(\(' + fnum + ', ' + fnum + '\), ' + fnum + '\)'
    m = re.match(circle_re, footprint)
    if m:
        return {'type': 'circle',
                'center': {'type': 'Point',
                           'coordinates': [float(m.groups()[0]),
                                           float(m.groups()[2])]},
                'radius': float(m.groups()[4])}
    else:
        raise ValueError('illegal footprint {}'.format(footprint))

