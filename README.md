```
/_____/\ /______/\  /_____/\ /__/\ /__/\  /_______/\/_____/\ /________/\
\:::_ \ \\::::__\/__\::::_\/_\::\ \\  \ \ \__.::._\/\::::_\/_\__.::.__\/
 \:(_) \ \\:\ /____/\\:\/___/\\::\/_\ .\ \   \::\ \  \:\/___/\  \::\ \
  \: ___\/ \:\\_  _\/ \_::._\:\\:: ___::\ \  _\::\ \__\:::._\/   \::\ \
   \ \ \    \:\_\ \ \   /____\:\\: \ \\::\ \/__\::\__/\\:\ \      \::\ \
    \_\/     \_____\/   \_____\/ \__\/ \::\/\________\/ \_\/       \__\/
```

Pipeline for Postgres pg_dump .sql file -> Redshift.

How it works
------------
pgshift reads a standard Postgres pg_dump file into a Pandas DataFrame, then chunks it and writes the chunks as gzipped csvs to S3 along with a Redshift `.manifest` file. It can then generate and execute the Redshift COPY statement to load the data into a specified table.

API
---

First create a Pandas DataFrame given a standard Postgres pg_dump .sql file:

```python
import pgshift
shifter = pgshift.process('pgshift/examples/example_dump.sql')

>>> shifter.table
         col1 col2 col3
0         one    1  foo
1         two    2  bar
2       three    3  baz
3        four    4  qux
4        five    5  foo
5         six    6  bar
6       seven    7  baz
7   eight   8  foo  NaN
8        nine    9  bar
9         ten   10  qux
10     eleven   11  bar
11     twelve   12  qux
```

Write the DataFrame in uniquely identified, gzipped CSV chunks to S3. COPY speed is *much* faster if chunks = multiple-of-Redshift-slices, so chunking is highly recommended for large data loads. Ex: DW1.XL nodes have 2 slices per node, so if running 2 nodes you will want chunks=4, 8, etc:

```python
>>> shifter.put_to_s3('mybucket', '/pgshift/', chunks=4)

Uploading mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_0.gz...
Uploading mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_1.gz...
Uploading mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_2.gz...
Uploading mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_3.gz...
Uploading manifest file mybucket/pgshift/pgshift_2cdf0cbb-b6e1-4616-948f-b7473f16b798.manifest...
```

The `.manifest` file will reference each of the S3 keys:
```json
{
    "entries": [
        {
            "mandatory": true,
            "url": "s3://mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_0.gz"
        },
        {
            "mandatory": true,
            "url": "s3://mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_1.gz"
        },
        {
            "mandatory": true,
            "url": "s3://mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_2.gz"
        },
        {
            "mandatory": true,
            "url": "s3://mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798_3.gz"
        }
    ]
}
```

Next, perform the COPY statement:

```python
>>> shifter.copy_to_redshift('mytable')

Connecting to Redshift...
COPYing data from s3://mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798.manifest into table mytable...
```

The COPY statement will look like the following:

```sql
COPY mytable
FROM 's3://mybucket/pgshift/pgdump_2cdf0cbb-b6e1-4616-948f-b7473f16b798.manifest'
CREDENTIALS 'aws_access_key_id=mycreds;aws_secret_access_key=mycreds'
MANIFEST
GZIP
CSV;
```

Finally, clean up the S3 bucket:

```python
>>> shifter.clean_up_s3()
```

Dependencies
------------
```
Cython==0.21.1
boto==2.34.0
gnureadline==6.3.3
numpy==1.9.1
pandas==0.15.0
psycopg2==2.5.4
python-dateutil==2.2
pytz==2014.9
```