import time
import concurrent.futures
from itertools import chain
import pandas as pd
import numpy as np
import os
# from delta import *
from typing import Iterable
from random import choice, shuffle

try:
    import cPickle as pickle
except:
    import pickle
import traceback
import pprint
import functools
from sqlalchemy import create_engine

pp = pprint.PrettyPrinter(depth=4)

# FAILED_SUCCESS = [1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
FAILED_SUCCESS = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    , 0, 0, 0, 0, 0, 0, 0, 1, 0]

ELT_PROCESS_VIEW = """ 
    select 
          a.id process_id
        , b.id source_id
        , f.id layer_id
        , c.id pipeline_id
        , h.id layer_pipeline_id
        , g.id source_pipeline_id

        , b."NAME" source_name
        , f.abbrev layer_name
        , a.apply_type
        , a.process_type

        , cast(b."LEVEL" as int) source_level
        , cast(f."LEVEL" as int) layer_level
        , cast(h."LEVEL" as int) layer_pipeline_level
        , cast(g."LEVEL" as int) source_pipeline_level
        , cast(a."LEVEL" as int) process_level

        , e2.id src_server_id
        , e2."NAME" src_server
        , d2.id src_db_id
        , d2.db_name src_db
        , a.source_table src_table

        , e1.id tgt_server_id
        , e1."NAME" tgt_server
        , d1.id tgt_db_id
        , d1.db_name tgt_db
        , a.target_table tgt_table

        , a."NAME" process_name

    from process a

        join source_pipeline g
        on g.id = a.source_pipeline_id
        and g.active = 1

        join data_source b
        on b.id = g.source_id
        and b.active = 1
        and b.scheduled = 1

        join layer_pipeline h
        on h.id = g.layer_pipeline_id

        join pipeline c        
        on c.id = h.pipeline_id
        and c.active = 1

        join db d1
        on d1.id = c.tgt_db_id

        join server e1
        on e1.id = d1.server_id

        join db d2
        on d2.id = c.src_db_id

        join server e2
        on e2.id = d2.server_id

        join layer f
        on f.id = h.layer_id

    where a.active = 1
    """

SOURCE_LOADS = """ 
            with t1 as 
            (
                select y.id source_id, x.load_id , min(cast(x.batch_id as int)) batch_seq
                from data_source_load x
                join data_source y 
                on upper(x.source_name) = upper(y."NAME")
                and y.id = '{}'

                {} -- exclude processed loads
                group by source_id, load_id
            )
            select x.* 
            from t1 x
            order by x.batch_seq
            """


# ELT_PROCESS_VIEW = """ select * from elt_process_view """
def exec_query(query, engine):
    try:
        return pd.read_sql_query(query, con=engine)
    except:
        print("Booom!!!!", query)


def Logging_decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except:
            print("Error!!", traceback.format_exc())

    return wrapper


def add_obj_to_dic(objx, i_dic):
    if objx.id not in i_dic:
        i_dic[objx.id] = objx
    else:
        objx = i_dic[objx.id]
    return objx


def sleep(i_sec=None):
    sec = 3 if i_sec is None else i_sec
    time.sleep(sec)


def replace_nan(df, replace_with):
    return df.replace(np.nan, replace_with, regex=True)


def threads(iterator, target_func):
    #     for i in iterator:
    #         target_func(i)
    ################################################################################
    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        executor.map(target_func, iterator)

################################################################################
#     threads = []

#     for i in iterator:
#         thread = threading.Thread(target=target_func, args=(i,))
#         thread.start()
#         threads.append(thread)

#     [thread.join() for thread in threads]
################################################################################
