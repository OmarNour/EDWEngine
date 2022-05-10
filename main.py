from model import *


class ETLRun:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers
        self.run_id = None
        self.start_time = None
        self.end_time = None
        self.time_elapsed = None
        self.source_failed = False
        self.execution_plan = {}
        self.registered_src_servers = {}
        self.registered_src_dbs = {}
        self.registered_tgt_servers = {}
        self.registered_tgt_dbs = {}
        self.registered_pipelines = {}
        self.registered_data_sources = {}
        self.registered_layers = {}
        self.registered_layer_pipelines = {}
        self.registered_src_pipelines = {}
        self.registered_processes = {}
        self.busy_target_tables = {}
        self.config_engine = create_engine('postgresql://postgres:postgres@localhost:5432/etl_config_db')

    @Logging_decorator
    def set_busy_target_tables(self, server=None, db=None, table_name=None, add=1):
        if server:
            if server not in self.busy_target_tables:
                self.busy_target_tables[server] = {}

            if db:
                if db not in self.busy_target_tables[server]:
                    self.busy_target_tables[server][db] = []

                if table_name:
                    if add:
                        self.busy_target_tables[server][db].append(table_name)
                    else:
                        self.busy_target_tables[server][db].remove(table_name)

    @Logging_decorator
    def get_busy_target_tables(self, server=None, db=None):
        if server and db:
            return self.busy_target_tables[server][db]

    @Logging_decorator
    def register_process(self, df_row):
        # MAKE dic FOR EACH OBJECT TO AVOID DUPLICATING OBJECTS!
        src_server = add_obj_to_dic(Server(df_row.src_server_id, df_row.src_server), self.registered_src_servers)
        src_db = add_obj_to_dic(Database(df_row.src_db_id, src_server, df_row.src_db), self.registered_src_dbs)
        tgt_server = add_obj_to_dic(Server(df_row.tgt_server_id, df_row.tgt_server), self.registered_tgt_servers)
        tgt_db = add_obj_to_dic(Database(df_row.tgt_db_id, tgt_server, df_row.tgt_db), self.registered_tgt_dbs)
        pipeline = add_obj_to_dic(Pipeline(df_row.pipeline_id, src_db, tgt_db), self.registered_pipelines)
        ds = add_obj_to_dic(DataSource(df_row.source_id, df_row.source_name, df_row.source_level), self.registered_data_sources)
        layer = add_obj_to_dic(Layer(df_row.layer_id, df_row.layer_name, df_row.layer_level), self.registered_layers)
        layer_pipeline = add_obj_to_dic(LayerPipeline(df_row.layer_pipeline_id, layer, pipeline, df_row.layer_pipeline_level), self.registered_layer_pipelines)
        src_pipeline = add_obj_to_dic(SourcePipeline(df_row.source_pipeline_id, layer_pipeline, ds, df_row.source_pipeline_level), self.registered_src_pipelines)

        if df_row.process_id not in self.registered_processes:
            process = Process(df_row.process_id, df_row.process_name, src_pipeline, df_row.src_table, df_row.tgt_table, df_row.apply_type, df_row.process_type, df_row.process_level)
            self.registered_processes[df_row.process_id] = process

        self.set_busy_target_tables(df_row.tgt_server_id, df_row.tgt_db_id)

    def prepare_execution_plan(self):
        for process in self.registered_processes.values():
            process_level = process.level
            source_pipeline = process.source_pipeline
            source_pipeline_level = source_pipeline.level
            layer_pipeline = source_pipeline.layer_pipeline
            layer_pipeline_level = layer_pipeline.level
            layer = layer_pipeline.layer
            layer_level = layer.level
            ds = source_pipeline.data_source
            ds_level = ds.level

            if layer_level not in ds.all_levels:
                ds.all_levels[layer_level] = {}
            if layer_pipeline_level not in ds.all_levels[layer_level]:
                ds.all_levels[layer_level][layer_pipeline_level] = {}
            if source_pipeline_level not in ds.all_levels[layer_level][layer_pipeline_level]:
                ds.all_levels[layer_level][layer_pipeline_level][source_pipeline_level] = {}
            if process_level not in ds.all_levels[layer_level][layer_pipeline_level][source_pipeline_level]:
                ds.all_levels[layer_level][layer_pipeline_level][source_pipeline_level][process_level] = []

            if process not in ds.all_levels[layer_level][layer_pipeline_level][source_pipeline_level][process_level]:
                ds.all_levels[layer_level][layer_pipeline_level][source_pipeline_level][process_level].append(process)

            if ds_level not in self.execution_plan:
                self.execution_plan[ds_level] = []
            if ds not in self.execution_plan[ds_level]:
                self.execution_plan[ds_level].append(ds)

    @Logging_decorator
    def register_all_processes(self):
        df = exec_query(ELT_PROCESS_VIEW, self.config_engine)
        for df_row in df.itertuples():
            self.register_process(df_row)

    #######################################################################################
    @Logging_decorator
    def run_process(self, p: Process):
        load_id = p.source_pipeline.data_source.current_load_id
        server_id = p.source_pipeline.layer_pipeline.pipeline.tgt_db.server.id
        tgt_db_id = p.source_pipeline.layer_pipeline.pipeline.tgt_db.id
        tgt_table = p.target_table

        table_processed = False
        while not table_processed:
            if tgt_table not in self.get_busy_target_tables(server_id, tgt_db_id):
                self.set_busy_target_tables(server_id, tgt_db_id, tgt_table)
                try:
                    assert self.get_busy_target_tables(server_id, tgt_db_id).count(tgt_table) == 1, "Warning: Target table counted more than once in busy list!, {}".format(tgt_table)
                    return_code, return_msg = p.run(load_id, self.run_id)
                    if return_code != 0:
                        p.source_pipeline.data_source.process_failed = True
                    self.set_busy_target_tables(server_id, tgt_db_id, tgt_table, 0)
                    table_processed = True
                except AssertionError as msg:
                    self.set_busy_target_tables(server_id, tgt_db_id, tgt_table, 0)
                    print(msg)

    #######################################################################################
    @Logging_decorator
    def run_source(self, i_data_source: DataSource):
        loads = i_data_source.get_loads(self.config_engine)
        for row in loads.itertuples():
            i_data_source.current_load_id = row.load_id
            if not i_data_source.process_failed:
                for level_of_layers in i_data_source.all_levels.keys():
                    if not i_data_source.process_failed:
                        layer_pipelines_dic = i_data_source.all_levels[level_of_layers]
                        for level_of_layer_pipelines in layer_pipelines_dic.keys():
                            if not i_data_source.process_failed:
                                source_pipelines_dic = layer_pipelines_dic[level_of_layer_pipelines]
                                for level_of_source_pipeline in source_pipelines_dic.keys():
                                    if not i_data_source.process_failed:
                                        process_dic = source_pipelines_dic[level_of_source_pipeline]
                                        for level_of_process in process_dic.keys():
                                            if not i_data_source.process_failed:
                                                processes = process_dic[level_of_process]
                                                threads(iterator=processes, target_func=self.run_process, max_workers=self.max_workers)
        self.source_failed = i_data_source.process_failed

    def generate_run_id(self):
        self.run_id = int(str(time.time()).replace('.', ''))

    @Logging_decorator
    def run(self):
        if not self.end_time:
            self.generate_run_id()
            self.start_time = time.time() if self.start_time is None else self.start_time
            self.register_all_processes()
            self.prepare_execution_plan()

            for level in self.execution_plan.keys():
                if not self.source_failed:
                    sources_in_level = self.execution_plan[level]
                    threads(iterator=sources_in_level, target_func=self.run_source, max_workers=None)

        self.end_time = time.time()
        self.time_elapsed = self.end_time - self.start_time


##################################################################################################################
if __name__ == '__main__':
    # run this in terminal id issue occurred related to libpq: "sudo ln -s /usr/lib/libpq.5.4.dylib /usr/lib/libpq.5.dylib"
    x = ETLRun()
    x.run()
    print("time_elapsed:", x.time_elapsed)
