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

FAILED_SUCCESS = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    , 0, 0, 0, 0, 0, 0, 0, 1, 0]
# FAILED_SUCCESS = [0]

ELT_PROCESS_VIEW = """ 
    select 
          p.id process_id
        , sp.id source_pipeline_id
        , dsl.id source_layer_id
        , l.id layer_id
        , ds.id source_id
        , pl.id pipeline_id
        

        , ds.source_name
        , l.abbrev layer_name
        , p.apply_type
    

        , cast(ds.source_level as int) source_level
        , cast(l.layer_level as int) layer_level
        , cast(dsl.ds_layer_level as int) data_source_layer_level
        , cast(sp.source_pipeline_level as int) source_pipeline_level
        , cast(p.process_level as int) process_level

        , e2.id src_server_id
        , e2.server_name src_server
        , d2.id src_db_id
        , d2.db_name src_db
        , src_t.id src_table_id
        , src_t.table_name src_table

        , e1.id tgt_server_id
        , e1.server_name tgt_server
        , d1.id tgt_db_id
        , d1.db_name tgt_db
        , tgt_t.id tgt_table_id
        , tgt_t.table_name tgt_table

    from processes p 

        join source_pipelines sp 
        on sp.id = p.source_pipeline_id
        and sp.active = 1
        
	        join data_source_layers dsl
	        on dsl.id = sp.source_layer_id
	        and dsl.active = 1
	
		        join data_sources ds
		        on ds.id = dsl.source_id
		        and ds.scheduled =1
		        and ds.active =1
		
			        join layers l
			        on l.id = dsl.layer_id
			        and l.active = 1

        join pipelines pl    
        on pl.id = sp.pipeline_id
        and pl.active = 1

	        join all_tables tgt_t
	        on tgt_t.id = pl.tgt_table_id
	        and tgt_t.active = 1
	
		        join db d1
		        on d1.id = tgt_t.db_id
		
			        join servers e1
			        on e1.id = d1.server_id

	        join all_tables src_t
	        on src_t.id = pl.src_table_id
	        and src_t.active = 1
	        
		        join db d2
		        on d2.id = src_t.db_id
	
			        join servers e2
			        on e2.id = d2.server_id

       
    where p.active = 1
    /*order by source_level
        ,layer_level
        ,data_source_layer_level
        ,source_pipeline_level
        ,process_level*/

    """

SOURCE_LOADS = """ 
            with t1 as 
            (
                select y.id source_id, x.load_id , min(cast(x.batch_id as int)) batch_seq
                from data_source_load x
                join data_sources y 
                on upper(x.source_name) = upper(y.source_name)
                and y.id = '{src_id}'
                {exclude_loads} -- exclude processed loads
                group by source_id, load_id
            )
            select x.* 
            from t1 x
            where batch_seq >= {current_batch_seq}
            --order by x.batch_seq
            """

CONFIG_ENGINE_NAME = "config_db"
CONFIG_USER_ID = "postgres"
CONFIG_PW = "postgres"
CONFIG_DB = "config_db"
CONFIG_HOST = "localhost"
CONFIG_PORT = 5432
SQL_ENGINE_DIC = {}


def Logging_decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except:
            print("Error!!", traceback.format_exc())

    return wrapper


# ELT_PROCESS_VIEW = """ select * from elt_process_view """
@Logging_decorator
def add_sql_engine(user, pw, host, port, db, engine_name):
    add_obj_to_dic(create_engine(f'postgresql://{user}:{pw}@{host}:{port}/{db}'), SQL_ENGINE_DIC, engine_name)


@Logging_decorator
def exec_query(query, engine_name) -> pd.DataFrame:
    engine = SQL_ENGINE_DIC[engine_name]
    try:
        return pd.read_sql_query(query, con=engine)
    except:
        print("Booom!!!!", query)
        print(traceback.format_exc())
        return pd.DataFrame()


@Logging_decorator
def add_obj_to_dic(obj, i_dic, key=None):
    if key is None:
        obj_key = obj.id
    else:
        obj_key = key

    if obj_key not in i_dic:
        i_dic[obj_key] = obj
    else:
        obj = i_dic[obj_key]
    return obj


def sleep(i_sec=None):
    sec = 3 if i_sec is None else i_sec
    time.sleep(sec)


def replace_nan(df, replace_with):
    return df.replace(np.nan, replace_with, regex=True)


def threads(iterator, target_func, max_workers=None):
    #     for i in iterator:
    #         target_func(i)
    ################################################################################
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(target_func, iterator)


################################################################################
#     threads = []

#     for i in iterator:
#         thread = threading.Thread(target=target_func, args=(i,))
#         thread.start()
#         threads.append(thread)

#     [thread.join() for thread in threads]
################################################################################

def get_files_in_dir(path, ext="", file_name=None):
    full_file_name = file_name + "." + ext if file_name is not None else None
    files = [name for name in os.listdir(path) if "." + ext in name
             and "~$" not in name
             and os.path.isfile(os.path.join(path, name))
             and (name == full_file_name or full_file_name is None)
             ]

    return files


def generate_run_id():
    return int(str(time.time()).replace('.', ''))


def is_dir_exists(path):
    return os.path.exists(path)


def create_dir(path):
    try:
        os.mkdir(os.path.join(path))
    except OSError:
        print("Creation of the directory %s failed" % path)
