
import os
import re

from contextlib import contextmanager


def convert_roi(roi):
    # Circle
    fnum = r'([-+]?\d+(\.\d*)?)'
    circle_re = r'circle\(\(' + fnum + ',' + fnum + r'\),' + fnum + r'\)'
    m = re.match(circle_re, roi.replace(' ', ''))
    if m:
        return {'type': 'Circle',
                'center': {'type': 'Point',
                           'coordinates': [float(m.groups()[0]),
                                           float(m.groups()[2])]},
                'radius': float(m.groups()[4])}
    else:
        raise ValueError('illegal roi {}'.format(roi))


@contextmanager
def chdir_context(new_dir):
    old_dir = os.getcwd()
    try:
        os.chdir(new_dir)
        yield
    finally:
        os.chdir(old_dir)


def str_to_bool(s):
    return s is not None and s.lower() in ('t', 'true', '1')


def find_exec(name):
    for p in os.environ.get('PATH', '').split(os.pathsep):
        full_path = os.path.join(p, name)
        if os.access(full_path, os.X_OK | os.R_OK):
            return full_path
    return None
