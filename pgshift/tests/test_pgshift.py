# -*- coding: utf-8 -*-
"""
PGShift Core Tests

Test Runner: PyTest

"""
import os
import unittest

import pandas as pd
import pandas.util.testing as pdt

import pgshift as pgs

TEST_DUMP = os.path.normpath(os.path.join(os.path.abspath(__file__),
                             '..', 'test_files', 'testdump.sql'))

class TestPGShift(unittest.TestCase):

    def test_get_rows(self):

        tbl = pgs.get_rows(TEST_DUMP)
        expected = pd.DataFrame(
            {'col1': ['one', 'two', 'three'],
             'col2': [1, 2, 3],
             'col3': ['foo', 'bar', 'baz']})
        pdt.assert_frame_equal(tbl, expected)

    def test_chunk_dataframe(self):

        df = pd.DataFrame(
            {'col1': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]}
            )
        chunked = pgs.chunk_dataframe(df, 1)
        pdt.assert_frame_equal(df, chunked[0])

        chunked = pgs.chunk_dataframe(df, 2)
        expected_0 = df[0:5]
        expected_1 = df[5:]
        pdt.assert_frame_equal(expected_0, chunked[0])
        pdt.assert_frame_equal(expected_1, chunked[1])

        df_2 = pd.DataFrame({'col1': [1, 2, 3, 4, 5, 6, 7, 8]})
        chunked = pgs.chunk_dataframe(df_2, 8)
        expected_0 = df_2[:1]
        expected_8 = df_2[-1:]
        self.assertEqual(len(chunked), 8)
        pdt.assert_frame_equal(expected_0, chunked[0])
        pdt.assert_frame_equal(expected_8, chunked[-1])

        chunked = pgs.chunk_dataframe(df_2, 3)
        expected_0 = df_2[:2]
        expected_1 = df_2[2:4]
        expected_2 = df_2[4:]
        for i, expect in enumerate([expected_0, expected_1, expected_2]):
            pdt.assert_frame_equal(chunked[i], expect)
