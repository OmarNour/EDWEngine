from functions import *


class Layer:
    def __init__(self, _id: str, abbrev: str, level: int):
        self._id = _id
        self.abbrev = abbrev
        self.level = level

    @property
    def id(self):
        return self._id


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
    def get_loads(self, engine):
        filter_load_id = ""
        df_loads = exec_query(SOURCE_LOADS.format(self._id, filter_load_id), engine)
        return df_loads


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


class Pipeline:
    def __init__(self, _id: str, src_db: Database, tgt_db: Database):
        self._id = _id
        self.tgt_db = tgt_db
        self.src_db = src_db

    @property
    def id(self):
        return self._id


class LayerPipeline:
    def __init__(self, _id: str, layer: Layer, pipeline: Pipeline, level: int):
        self._id = _id
        self.layer = layer
        self.pipeline = pipeline
        self.level = level

    @property
    def id(self):
        return self._id


class SourcePipeline:
    def __init__(self, _id: str, layer_pipeline: LayerPipeline, data_source: DataSource, level: int):
        self._id = _id
        self.data_source = data_source
        self.layer_pipeline = layer_pipeline
        self.level = level
        # self.processes = []

    @property
    def id(self):
        return self._id


class Process:
    def __init__(self, _id: str, name: str, source_pipeline: SourcePipeline, source_table: str, target_table: str, apply_type: str, process_type: str, level: int):
        self.run_id = None
        self._id = _id
        self.name = name
        self.source_pipeline = source_pipeline
        self.process_type = process_type
        self.process_type = process_type
        self.apply_type = apply_type
        self.source_table = source_table
        self.target_table = target_table
        self.level = level
        self.last_load_id = None
        self.passed = None

    def run(self, load_id: str, run_id: int):
        self.last_load_id = load_id
        self.run_id = run_id
        # **********************************************************************#
        # check for the process status in the last run
        # instead of sleep(n), call the DB procedure, to run the process
        #
        sleep(1)
        return_code = choice(FAILED_SUCCESS)
        return_msg = ":)" if return_code == 0 else ":("
        if return_code == 0:
            self.passed = True
        else:
            self.passed = False
        # **********************************************************************#
        source_id = self.source_pipeline.data_source.id
        layer_id = self.source_pipeline.layer_pipeline.layer.id

        print('Result:{} - {}'.format(return_code, return_msg)
              , 'Source: {}'.format(source_id)
              , 'Load: {}'.format(load_id)
              , 'Layer: {}'.format(layer_id)
              , 'Process: {}'.format(self._id), sep='\t')

        return return_code, return_msg
