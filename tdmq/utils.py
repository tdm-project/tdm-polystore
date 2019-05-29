import re


def convert_footprint(footprint):
    # Circle
    fnum = r'([-+]?\d+(\.\d*)?)'
    circle_re = r'circle\(\(' + fnum + ', ' + fnum + r'\), ' + fnum + r'\)'
    m = re.match(circle_re, footprint)
    if m:
        return {'type': 'circle',
                'center': {'type': 'Point',
                           'coordinates': [float(m.groups()[0]),
                                           float(m.groups()[2])]},
                'radius': float(m.groups()[4])}
    else:
        raise ValueError('illegal footprint {}'.format(footprint))
