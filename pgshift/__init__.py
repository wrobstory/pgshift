# -*- coding: utf-8 -*-
try:
    from pgshift.pgshift import get_rows, process, chunk_dataframe
except ImportError:
    from pgshift import get_rows, process, chunk_dataframe