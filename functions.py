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


SOURCES_IN_LEVEL = """ 
                        select distinct source_id
                        from elt_process_view 
                        where source_level = {}
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


##################################################################################################################
def get_files_in_dir(path, ext="", file_name=None):
    full_file_name = file_name + "." + ext if file_name is not None else None
    files = [name for name in os.listdir(path) if "." + ext in name
             and "~$" not in name
             and os.path.isfile(os.path.join(path, name))
             and (name == full_file_name or full_file_name is None)
             ]

    return files


def sleep(i_sec=None):
    sec = 3 if i_sec is None else i_sec
    time.sleep(sec)


def replace_nan(df, replace_with):
    return df.replace(np.nan, replace_with, regex=True)


def save_df_as_parquet(df, path):
    df.write.mode('overwrite').parquet(path)


def drop_table(i_spark, table_name):
    i_spark.catalog.dropTempView(table_name)


def create_table(i_spark, df, table_name, commit=False):
    drop_table(i_spark, table_name)
    df.createTempView(table_name)
    if commit:
        save_df_as_parquet(df, "tables/{}.parquet".format(table_name))


def withColumn_id(df):
    return df.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())))


def melt(
        df,
        id_vars: Iterable[str],
        value_vars: Iterable[str],
        var_name: str = "variable",
        value_name: str = "value"
):
    """Convert :class:`DataFrame` from wide to long format."""

    # Create map<key: value>
    _vars_and_vals = create_map(
        list(chain.from_iterable([
            [F.lit(c), col(c)] for c in value_vars]
        ))
    )

    _tmp = df.select(*id_vars, explode(_vars_and_vals)) \
        .withColumnRenamed('key', var_name) \
        .withColumnRenamed('value', value_name)

    return _tmp


def prepare_df(pdf):
    df = replace_nan(pdf, '')
    df = pdf.applymap(lambda x: str(x).strip())
    cols = list(df.columns.values)
    cols_without_spc = [col.replace(" ", "_") for col in cols]
    df.columns = cols_without_spc
    return df


def pandas_to_spark(df):
    pdf = prepare_df(df)
    return sqlContext.createDataFrame(pdf)


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
