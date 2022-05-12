import pandas as pd

from functions import *


# ERD: https://lucid.app/lucidchart/b806d35b-7360-43cd-80e5-e51c5d64c7c5/edit?page=0_0&invitationId=inv_ed4c6708-54d4-47f3-bb5c-51aefa3f8199#
class DataSource:
    def __init__(self, _id: str, name: str, level: int):
        self.process_failed = False
        self._id = _id
        self.name = name
        self.level = level
        self.all_levels = {}
        self.current_load_id = None

    @property
    def id(self):
        return self._id

    @Logging_decorator
    def get_loads(self, engine) -> pd.DataFrame:
        filter_load_id = ""
        df_loads = exec_query(SOURCE_LOADS.format(src_id=self._id, exclude_loads=filter_load_id), engine)
        return df_loads


class Layer:
    def __init__(self, _id: str, abbrev: str, level: int):
        self._id = _id
        self.abbrev = abbrev
        self.level = level

    @property
    def id(self):
        return self._id


class DataSourceLayer:
    def __init__(self, _id: str, data_source: DataSource, layer: Layer, level: int):
        self._id = _id
        self.level = level
        self.data_source = data_source
        self.layer = layer

    @property
    def id(self):
        return self._id


class Server:
    def __init__(self, _id: str, name: str):
        self._id = _id
        self.name = name

    @property
    def id(self):
        return self._id


class ServerIp:
    def __init__(self, _id: str, server: Server, ip: str):
        self._id = _id
        self.server = server
        self.ip = ip


class Database:
    def __init__(self, _id: str, server: Server, db_name: str):
        self._id = _id
        self.server = server
        self.db_name = db_name

    @property
    def id(self):
        return self._id


class Table:
    def __init__(self, _id: str, database: Database, table_name: str):
        self._id = _id
        self.database = database
        self.table_name = table_name

    @property
    def id(self):
        return self._id


class Pipeline:
    def __init__(self, _id: str, src_table: Table, tgt_table: Table):
        self._id = _id
        self.src_table = src_table
        self.tgt_table = tgt_table

    @property
    def id(self):
        return self._id


class SourcePipeline:
    def __init__(self, _id: str, pipeline: Pipeline, data_source_layer: DataSourceLayer, level: int):
        self._id = _id
        self.pipeline = pipeline
        self.data_source_layer = data_source_layer
        self.level = level

    @property
    def id(self):
        return self._id


class Process:
    def __init__(self, _id: str, name: str, source_pipeline: SourcePipeline, apply_type: str, process_type: str, level: int):
        self._id = _id
        self.name = name
        self.source_pipeline = source_pipeline
        self.process_type = process_type
        self.apply_type = apply_type
        self.level = level
        self.passed = None

    @property
    def id(self):
        return self._id

    def run(self, run_id):
        current_load_id = self.source_pipeline.data_source_layer.data_source.current_load_id
        # **********************************************************************#
        # TODO:
        #   instead of sleep(n), call the DB procedure, to run the process
        #

        sleep(1)
        return_code = choice(FAILED_SUCCESS)
        return_msg = ":)" if return_code == 0 else ":("
        if return_code == 0:
            self.passed = True
        else:
            self.source_pipeline.data_source_layer.data_source.process_failed = True
            self.passed = False
        # **********************************************************************#
        source_id = self.source_pipeline.data_source_layer.data_source.id
        layer_id = self.source_pipeline.data_source_layer.layer.id

        print('Result:{} - {}'.format(return_code, return_msg)
              , 'Source: {}'.format(source_id)
              , 'Load: {}'.format(current_load_id)
              , 'Layer: {}'.format(layer_id)
              , 'Process: {}'.format(self._id), sep='\t')

        return return_code, return_msg
