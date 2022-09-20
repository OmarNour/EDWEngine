import time
import concurrent.futures
from itertools import chain
import pandas as pd
import numpy as np
import os
# from delta import *
from typing import Iterable
from random import choice, shuffle, random, randint
import csv
import psycopg2
import asyncio

try:
    import cPickle as pickle
except:
    import pickle
import traceback
import pprint
import functools
from sqlalchemy import create_engine
import configparser

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
                on upper(x.source_id) = upper(y.id)
                and y.id = '{src_id}'
                {exclude_loads} -- exclude processed loads
                group by y.id, load_id
            )
            select x.* 
            from t1 x
            where batch_seq >= {current_batch_seq}
            --order by x.batch_seq
            """

# CONFIG_ENGINE_NAME = None
# CONFIG_USER_ID = None
# CONFIG_PW = None
# CONFIG_DB = None
# CONFIG_HOST = None
# CONFIG_PORT = None
SQL_ENGINE_DIC = {}


def PopulateFromConfigFile(self):
    config_file = 'config_file'
    parser = configparser.ConfigParser()
    parser.read(config_file)
    for section in parser.sections():
        if section == "configurations":
            for key, value in parser.items(section):
                value = value.replace('"', '')
                key = key.upper()
                # print(' {} = {}'.format(key, value))
                if key == "CONFIG_ENGINE_NAME":
                    self.CONFIG_ENGINE_NAME = value
                if key == "CONFIG_USER_ID":
                    self.CONFIG_USER_ID = value
                if key == "CONFIG_PW":
                    self.CONFIG_PW = value
                if key == "CONFIG_DB":
                    self.CONFIG_DB = value
                if key == "CONFIG_HOST":
                    self.CONFIG_HOST = value
                if key == "CONFIG_HOST":
                    self.CONFIG_HOST = value
                if key == "CONFIG_PORT":
                    self.CONFIG_PORT = value


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
        print(f"{path} folder already exists!")


def open_csv_file(file_location: str) -> iter:
    with open(file_location) as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            yield row


def get_third_col(row: iter) -> str:
    return [row[3]]


def stream_csv_rows(rows: iter, transform_func: object) -> iter:
    for row in rows:
        transformed_row = transform_func(row)
        yield transformed_row


def write_csv_rows(rows: iter, file_location: str, header=None):
    with open(file_location, 'w') as csvfile:
        writer = csv.writer(csvfile)
        if header is not None:
            writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def generate_random_int(min_val=0, max_val=999):
    return randint(min_val, max_val)


def generate_id():
    # return int(str(time.time()).replace('.', ''))
    return time.time_ns() + generate_random_int()


def insert_into_db(id, session_id):
    conn = psycopg2.connect("dbname=retail_db user=postgres password=postgres")
    cur = conn.cursor()
    cur.execute("""insert into public.test_unq_id ( id, session_id) values ({}, {})""".format(id, session_id))
    conn.commit()
    # cur.execute('SELECT * from public.test_unq_id')
    # rows = cur.fetchall()
    # db_version = cur.fetchone()
    # print(rows)
    cur.close()


async def always_insert(session_id):
    while True:
        insert_into_db(generate_id(), session_id)
        await asyncio.sleep(0.01)


async def concurrent_inserts():
    """Runs the Producer and Consumer tasks"""
    # task_list = []
    for i in range(1000):
        asyncio.create_task(always_insert(i))
    #     task_list.append(t)
    #     print(i)
    for tx in asyncio.tasks.all_tasks():
        await tx
    # t1 = asyncio.create_task(always_insert(1))
    # t2 = asyncio.create_task(always_insert(2))
    # await t1
    # await t2


def main():
    try:
        asyncio.run(concurrent_inserts())
        # threads(iterator, target_func, max_workers=None)
    except KeyboardInterrupt as e:
        print("shutting down")


def load_excelFile_to_db(file_path, user, pw, host, db, schema, port):
    db_uri = f'postgresql://{user}:{pw}@{host}:{port}/{db}'
    db_engine = create_engine(url=db_uri, encoding='utf-8')
    xls = pd.ExcelFile(file_path)
    sheets = xls.sheet_names
    for sheet_name in sheets:
        try:
            df = xls.parse(sheet_name).replace(np.nan, value='', regex=True)
            records_counts = len(df.index)
            if records_counts > 0:
                df.columns = df.columns.str.lower().str.replace('  ', ' ').str.replace(' ', '_')
                df = df.applymap(lambda x: x.strip() if type(x) is str else int(x) if type(x) is float else x)
                table_name = sheet_name.replace('  ', ' ').replace(' ', '_').lower()
                df.drop_duplicates().to_sql(table_name, con=db_engine, if_exists='replace', schema=schema, index=False)
                print(f'{records_counts} record from sheet "{sheet_name}", moved to {schema}.{table_name} table.')
            else:
                print(f'Sheet "{sheet_name}" is empty!.')
        except Exception as e:
            print(f'Failed to read sheet "{sheet_name}" :(')
            print(e)


if __name__ == '__main__':
    # print(generate_id())
    main()

    # folder = "/Users/omarnour/Downloads/"
    # csv_file = "data-1581886013539.csv"
    # trx_file = "data-1581886013539_trx.csv"
    # rows = open_csv_file(folder + csv_file)
    # transformed_rows = stream_csv_rows(rows, get_third_col)
    # write_csv_rows(transformed_rows, folder + trx_file)
    # print("Done")
    # for row in rows:
    #     print(row)
    #     print("\n")
