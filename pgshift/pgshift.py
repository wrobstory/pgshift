# -*- coding: utf-8 -*-
"""

pgshift: write a Postgres pg_dump .sql file to Redshift via S3

"""
from __future__ import division, print_function

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

import gzip
import json
import math
import os
import re

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin
import uuid

from boto.s3.connection import S3Connection
import pandas as pd
import psycopg2


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

    tbl = pd.read_table(StringIO(str_blob), delimiter='\t',
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
    chunker = list(range(chunk_size, len(df), chunk_size)) or [chunk_size]
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
        self.manifest_url = None

    def put_to_s3(self, bucket_name, keypath, chunks=1, aws_access_key_id=None,
                  aws_secret_access_key=None, mandatory_manifest=True):
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
        mandatory_manifest: bool, default True
            Should .manifest entries be mandatory?
        """
        if aws_access_key_id and aws_secret_access_key:
            self.conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        else:
            self.conn = S3Connection()

        self.bucket = self.conn.get_bucket(bucket_name)

        self.manifest = {'entries': []}
        self.generated_keys = []
        table_chunks = chunk_dataframe(self.table, chunks)
        batch_uuid = str(uuid.uuid4())
        for idx, chunk in enumerate(table_chunks):
            zipname = '_'.join(['pgdump', batch_uuid, str(idx)]) + '.gz'
            fp, gzfp = StringIO(), StringIO()
            csvd = chunk.to_csv(fp, index=False, header=False)
            fp.seek(0)
            url = urljoin(keypath, zipname)
            key = self.bucket.new_key(url)
            self.generated_keys.append(url)
            gzipped = gzip.GzipFile(fileobj=gzfp, mode='w')
            gzipped.write(fp.read())
            gzipped.close()
            gzfp.seek(0)
            print('Uploading {}...'.format(self.bucket.name + url))
            key.set_contents_from_file(gzfp)

            self.manifest['entries'].append({
                'url': ''.join(['s3://', self.bucket.name, url]),
                'mandatory': mandatory_manifest}
                )

        manifest_name = 'pgshift_{}.manifest'.format(batch_uuid)
        fest_url = urljoin(keypath, manifest_name)
        self.generated_keys.append(fest_url)
        self.manifest_url = ''.join(['s3://', self.bucket.name, fest_url])
        fest_key = self.bucket.new_key(fest_url)
        fest_fp = StringIO(json.dumps(self.manifest, sort_keys=True,
                                      indent=4))
        fest_fp.seek(0)
        print('Uploading manifest file {}...'.format(
            self.bucket.name + fest_url))
        fest_key.set_contents_from_file(fest_fp)

    def clean_up_s3(self):
        """Clean up S3 keys generated in `put_to_s3`"""
        for key in self.generated_keys:
            print('Deleting {}...'.format(self.bucket.name + key))
            self.bucket.delete_key(key)

    def copy_to_redshift(self, table_name, aws_access_key_id=None,
                         aws_secret_access_key=None, database=None, user=None,
                         password=None, host=None, port=None, sslmode=None):
        """
        COPY data from S3 to Redshift using the data and manifest generated
        with `put_to_s3`, which must be called first in order to
        perform the COPY statement.

        Parameters
        ----------
        table_name: str
            Table name to copy data to
        aws_access_key_id: str
        aws_secret_access_key: str
        database: str, if None os.environ.get('PGDATABASE')
        user: str, if None os.environ.get('PGUSER')
        password: str, if None os.environ.get('PGPASSWORD')
        host: str, if None os.environ.get('PGHOST')
        port: int, if None os.environ.get('PGPORT') or 5439
        sslmode: str
            sslmode param (ex: 'require', 'prefer', etc)
        """
        aws_secret_access_key = (aws_secret_access_key
                                 or os.environ.get('AWS_SECRET_ACCESS_KEY'))
        aws_access_key_id = (aws_access_key_id
                             or os.environ.get('AWS_ACCESS_KEY_ID'))

        database = database or os.environ.get('PGDATABASE')
        user = user or os.environ.get('PGUSER')
        password = password or os.environ.get('PGPASSWORD')
        host = host or os.environ.get('PGHOST')
        port = port or os.environ.get('PGPORT') or 5439

        print('Connecting to Redshift...')
        self.conn = psycopg2.connect(database=database, user=user,
                                     password=password, host=host,
                                     port=port, sslmode='require')

        self.cur = self.conn.cursor()

        query = """COPY {0}
                   FROM '{1}'
                   CREDENTIALS 'aws_access_key_id={2};aws_secret_access_key={3}'
                   MANIFEST
                   GZIP
                   CSV;""".format(table_name, self.manifest_url,
                                  aws_access_key_id, aws_secret_access_key)
        print("COPYing data from {} into table {}...".format(self.manifest_url,
                                                             table_name))
        self.cur.execute(query)
        self.conn.commit()
        self.conn.close()
