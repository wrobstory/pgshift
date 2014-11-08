# -*- coding: utf-8 -*-
"""

pgshift: write a Postgres pg_dump .sql file to Redshift via S3

"""
from __future__ import division, print_function

import cStringIO
import math
import re
import uuid

import pandas as pd

def get_rows(path):
    """
    Given a path to a pg_dump .sql file, return a list of dicts, ex:
    [{'col1': 'one', 'col2': 1, 'col3': 'foo'}]

    Parameters
    ----------
    path: string
        Path to pg_dump .sql file
    """
    col_matcher = re.compile('COPY.+\((.+)\) FROM stdin;')

    str_blob = ''
    read_lines = False
    with open(path, 'r') as fread:

        for line in fread:
            match = col_matcher.match(line)

            if '\.\n' in line:
                read_lines = False

            if read_lines:
                str_blob = ''.join([str_blob, line])

            if match:
                read_lines = True
                col_keys = [c.strip() for c in match.groups()[0].split(',')]

    tbl = pd.read_table(cStringIO.StringIO(str_blob), delimiter='\t',
                        names=col_keys)

    return tbl

def process(filepath):
    """
    Process a given pg_dump into a pgshift result that you can use to
    write to S3, or perform a COPY statement in Redshift

    Parameters
    ----------
    filename: str
        Path to pg_dump .sql file
    """

    return PGShift(get_rows(filepath))

def chunk_dataframe(df, num_chunks):
    """Chunk DataFrame into `chunks` DataFrames in a list"""
    chunk_size = int(math.floor(len(df) / num_chunks)) or 1
    chunker = range(chunk_size, len(df), chunk_size) or [chunk_size]
    if len(df) == num_chunks:
        chunker.append(len(df))
    last_iter = 0
    df_list = []
    for c in chunker:
        if c == chunker[-1]:
            c = len(df)
        df_list.append(df[last_iter:c])
        last_iter = c
    return df_list

class PGShift(object):

    def __init__(self, table):
        self.table = table

    def put_to_s3(self, bucket_name, keypath, chunks=1, aws_access_key_id=None,
                  aws_secret_access_key=None):
        """
        Will put the result table to S3 as a gzipped CSV with an accompanying
        .manifest file. The aws keys are not required if you have environmental
        params set for boto to pick up:
        http://boto.readthedocs.org/en/latest/s3_tut.html#creating-a-connection

        Each call to this function will generate a unique UUID for that
        particular run.

        Ex: If bucket is 'mybucket', keypath is 'pgshift/temp/',
        and chunks is 2, then will write the following:
        s3://mybucket/pgshift/temp/pgdump_uuid_0.gz
        s3://mybucket/pgshift/temp/pgdump_uuid_1.gz
        s3://mybucket/pgshift/temp/pgtemp.manifest

        Parameters
        ----------
        bucket_name: str
            S3 bucket name
        keypath: str
            Key path for writing file
        chunks: int, default 1
            Number of gzipped chunks to write. Upload speed
            is *much* faster if chunks = multiple-of-slices. Ex: DW1.XL nodes
            have 2 slices per node, so if running 2 nodes you will want
            chunks=4, 8, etc
        aws_access_key_id: str, default None
        aws_secret_access_key: str, default None
        """
        if aws_access_key_id and aws_secret_access_key:
            self.conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        else:
            self.conn = S3Connection()

        bucket = self.conn.get_bucket(bucket_name)

        manifested = []
        table_chunks = chunk_dataframe(self.table, chunks)
        for chunk in table_chunks:
            zipname = '_'.join(['pgdump', str(uuid.uuid4()), str(chunk)])