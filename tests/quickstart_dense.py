#!/usr/bin/env python3

# quickstart_dense.py
#
# LICENSE
#
# The MIT License
#
# Copyright (c) 2018 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# DESCRIPTION
#
# This is a part of the TileDB quickstart tutorial:
#   https://docs.tiledb.io/en/latest/tutorials/quickstart.html
#
# When run, this program will create a simple 2D dense array, write some data
# to it, and read a slice of the data back.


import argparse
import os
from contextlib import contextmanager
from urllib.parse import urlparse

import numpy as np
import tiledb


def create_array(array_name):
    # The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
    dom = tiledb.Domain(tiledb.Dim(name="rows", domain=(1, 4), tile=4, dtype=np.int32),
                        tiledb.Dim(name="cols", domain=(1, 4), tile=4, dtype=np.int32))

    # The array will be dense with a single attribute "a" so each (i,j) cell can store an integer.
    schema = tiledb.ArraySchema(domain=dom, sparse=False,
                                attrs=[tiledb.Attr(name="a", dtype=np.int32)])

    # Create the (empty) array on disk.
    tiledb.DenseArray.create(array_name, schema)


def write_array(array_name):
    # Open the array and write to it.
    with tiledb.DenseArray(array_name, mode='w') as A:
        data = np.array(([1, 2, 3, 4],
                         [5, 6, 7, 8],
                         [9, 10, 11, 12],
                         [13, 14, 15, 16]))
        A[:] = data


def read_array(array_name):
    # Open the array and read from it.
    with tiledb.DenseArray(array_name, mode='r') as A:
        # Slice only rows 1, 2 and cols 2, 3, 4.
        data = A[1:3, 2:5]
        print(data["a"])


def s3_context(vfs, array_name):
    url_parts = urlparse(array_name)
    bucket_url = f"s3://{url_parts.netloc}"

    new_bucket = False
    if not vfs.is_bucket(bucket_url):
        vfs.create_bucket(bucket_url)
        new_bucket = True

    try:
        yield
    finally:
        if new_bucket:
            vfs.empty_bucket(bucket_url)
            vfs.remove_bucket(bucket_url)


@contextmanager
def storage_context(vfs, array_name):
    if array_name.startswith('s3:'):
        yield s3_context(vfs, array_name)
    else:
        yield


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", dest="file", default="s3://quickdense/quickstart_dense")
    tiledb.default_ctx(config={
        "vfs.s3.aws_access_key_id": os.environ.get('AWS_ACCESS_KEY_ID', ''),
        "vfs.s3.aws_secret_access_key": os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
        "vfs.s3.endpoint_override": os.environ.get('AWS_S3_ENDPOINT', ''),
        "vfs.s3.region": os.environ.get('AWS_DEFAULT_REGION', ""),
        "vfs.s3.scheme": "http",
        "vfs.s3.verify_ssl": "false",
        "vfs.s3.use_virtual_addressing": "false",
        "vfs.s3.use_multipart_upload": "false",
    })
    vfs = tiledb.VFS()

    args = parser.parse_args()
    array_name = args.file

    with storage_context(vfs, array_name):
        array_created = False
        if tiledb.object_type(array_name) != "array":
            create_array(array_name)
            array_created = True
            write_array(array_name)

        read_array(array_name)

        if array_created:
            vfs.remove_dir(array_name)

if __name__ == '__main__':
    main()
