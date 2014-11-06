# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='pgshift',
    version='0.0.1',
    description='Postgres pg_dump -> Redshift',
    author='Rob Story',
    author_email='wrobstory@gmail.com',
    license='MIT License',
    url='https://github.com/wrobstory/pgshift',
    keywords='Postgres Redshift',
    classifiers=['Development Status :: 4 - Beta',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 3',
                 'License :: OSI Approved :: MIT License'],
    packages=['pgshift']
)