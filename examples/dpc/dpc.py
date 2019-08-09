# dpc specific ingestion support
import numpy as np
import tifffile as tiff
import math
import tempfile
import requests
import os
from datetime import datetime


dpc_url = \
    'http://www.protezionecivile.gov.it/wide-api/wide/product/downloadProduct'

mask_value = -9999.0


def extract_data(tif):
    page = tif.pages[0]
    data = page.asarray()
    # zeroout nan, force mask value to -9999.0
    data[np.isnan(data)] = mask_value
    return page.geotiff_tags, data


def gather_data(t, field):
    # FIXME either datetime or np.datetime64[*]
    t = t if isinstance(t, datetime) else t.tolist()
    product_date = math.floor(t.timestamp() * 1000)
    payload = {'productType': field, 'productDate': product_date}
    print(dpc_url, payload)
    r = requests.post(dpc_url, json=payload)
    if r.status_code != 200:
        raise ValueError(f"Bad return code: {r.status_code}")
    # FIXME find a cleaner way to handle the returned tif file.
    handle, fpath = tempfile.mkstemp()
    with open(fpath, 'wb') as f:
        f.write(r.content)
    tif = tiff.TiffFile(fpath)
    os.unlink(fpath)
    return extract_data(tif)


def fetch_dpc_data(source, t, field):
    try:
        g, d = gather_data(t, field)
    except ValueError as e:
        print(f'adding masked data for {t} {field} because of {e}.')
        d = np.zeros(source.shape)
        d.fill(mask_value)
    return d
