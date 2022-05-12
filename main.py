from model import *


class ETLRun:
    def __init__(self, max_workers=None, config_user_id="postgres", config_pw="postgres", config_db="config_db", host="localhost", port=5432):
        self.registered_ds_layers = {}
        self.registered_tgt_tbls = {}
        self.registered_src_tbls = {}
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
        self.global_target_table = {}
        self.config_engine = create_engine(f'postgresql://{config_user_id}:{config_pw}@{host}:{port}/{config_db}')

    @Logging_decorator
    def register_process(self, df_row):
        # MAKE dic FOR EACH OBJECT TO AVOID DUPLICATING OBJECTS!
        src_server = add_obj_to_dic(Server(df_row.src_server_id, df_row.src_server), self.registered_src_servers)
        src_db = add_obj_to_dic(Database(df_row.src_db_id, src_server, df_row.src_db), self.registered_src_dbs)
        src_tbl = add_obj_to_dic(Table(df_row.src_table_id, src_db, df_row.src_table), self.registered_src_tbls)

        tgt_server = add_obj_to_dic(Server(df_row.tgt_server_id, df_row.tgt_server), self.registered_tgt_servers)
        tgt_db = add_obj_to_dic(Database(df_row.tgt_db_id, tgt_server, df_row.tgt_db), self.registered_tgt_dbs)
        tgt_tbl = add_obj_to_dic(Table(df_row.tgt_table_id, tgt_db, df_row.tgt_table), self.registered_tgt_tbls)

        pipeline = add_obj_to_dic(Pipeline(df_row.pipeline_id, src_tbl, tgt_tbl), self.registered_pipelines)

        ds = add_obj_to_dic(DataSource(df_row.source_id, df_row.source_name, df_row.source_level), self.registered_data_sources)
        layer = add_obj_to_dic(Layer(df_row.layer_id, df_row.layer_name, df_row.layer_level), self.registered_layers)
        ds_layer = add_obj_to_dic(DataSourceLayer(df_row.source_layer_id, ds, layer, df_row.data_source_layer_level), self.registered_ds_layers)

        src_pipeline = add_obj_to_dic(SourcePipeline(df_row.source_pipeline_id, pipeline, ds_layer, df_row.source_pipeline_level), self.registered_src_pipelines)

        if df_row.process_id not in self.registered_processes:
            process = Process(df_row.process_id, '', src_pipeline, df_row.apply_type, '', df_row.process_level)
            self.registered_processes[df_row.process_id] = process

    def prepare_execution_plan(self):
        for process in self.registered_processes.values():
            source_pipeline = process.source_pipeline
            source_pipeline_level = source_pipeline.level

            target_table_id = source_pipeline.pipeline.tgt_table.id
            process_level = process.level

            ds_layer = source_pipeline.data_source_layer
            ds_layer_level = ds_layer.level

            layer = ds_layer.layer
            layer_level = layer.level

            ds = ds_layer.data_source
            ds_level = ds.level

            if layer_level not in ds.all_levels:
                ds.all_levels[layer_level] = {}

            if ds_layer_level not in ds.all_levels[layer_level]:
                ds.all_levels[layer_level][ds_layer_level] = {}

            if source_pipeline_level not in ds.all_levels[layer_level][ds_layer_level]:
                ds.all_levels[layer_level][ds_layer_level][source_pipeline_level] = {}

            if process_level not in ds.all_levels[layer_level][ds_layer_level][source_pipeline_level]:
                ds.all_levels[layer_level][ds_layer_level][source_pipeline_level][process_level] = {}

            if target_table_id not in ds.all_levels[layer_level][ds_layer_level][source_pipeline_level][process_level]:
                ds.all_levels[layer_level][ds_layer_level][source_pipeline_level][process_level][target_table_id] = []

            if process not in ds.all_levels[layer_level][ds_layer_level][source_pipeline_level][process_level][target_table_id]:
                ds.all_levels[layer_level][ds_layer_level][source_pipeline_level][process_level][target_table_id].append(process.id)

            if ds_level not in self.execution_plan:
                self.execution_plan[ds_level] = []

            if ds not in self.execution_plan[ds_level]:
                self.execution_plan[ds_level].append(ds)

            if target_table_id not in self.global_target_table:
                self.global_target_table[target_table_id] = []

    @Logging_decorator
    def register_all_processes(self):
        df = exec_query(ELT_PROCESS_VIEW, self.config_engine)
        for df_row in df.itertuples():
            self.register_process(df_row)

    #######################################################################################
    def get_process(self, process_id) -> Process:
        return self.registered_processes[process_id]

    @Logging_decorator
    def run_process(self, process_id):
        p = self.get_process(process_id)
        p.run(self.run_id)
        self.global_target_table[p.source_pipeline.pipeline.tgt_table.id].remove(p.id)

    #######################################################################################

    @Logging_decorator
    def run_source(self, i_data_source: DataSource):

        def wait_for_result(i_target_table_id, process):
            while process in self.global_target_table[i_target_table_id]:
                pass

        def send_to_run(i_target_table_id):
            for process_id in target_table_dic[i_target_table_id]:
                self.global_target_table[i_target_table_id].append(process_id)
                wait_for_result(i_target_table_id, process_id)

        loads = i_data_source.get_loads(self.config_engine)
        if not loads.empty:
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
                                                    target_table_dic = process_dic[level_of_process]
                                                    threads(iterator=target_table_dic.keys(), target_func=send_to_run, max_workers=self.max_workers)
            self.source_failed = i_data_source.process_failed

    @Logging_decorator
    def generate_run_id(self):
        self.run_id = int(str(time.time()).replace('.', ''))

    @Logging_decorator
    def run_all_sources(self):
        if not self.end_time:
            self.start_time = time.time() if self.start_time is None else self.start_time

            self.generate_run_id()
            self.register_all_processes()
            self.prepare_execution_plan()

            for level in self.execution_plan.keys():
                if not self.source_failed:
                    sources_in_level = self.execution_plan[level]
                    threads(iterator=sources_in_level, target_func=self.run_source, max_workers=None)

        self.end_time = time.time()
        self.time_elapsed = self.end_time - self.start_time

    def run_target_table_processes(self):
        def run_target_table(i_target_table):
            for process_id in self.global_target_table[i_target_table]:
                self.run_process(process_id)

        while self.end_time is None:
            threads(iterator=self.global_target_table.keys(), target_func=run_target_table, max_workers=None)

    def run(self, run_seq):
        if run_seq == 0:
            self.run_target_table_processes()

        if run_seq == 1:
            self.run_all_sources()

    def main(self):
        threads(iterator=[0, 1], target_func=x.run, max_workers=None)
        print(f"RunID: {self.run_id}, time_elapsed: {self.time_elapsed}")


##################################################################################################################
if __name__ == '__main__':
    # TODO:
    #   add concurrency parameter to be passed to max_worker parameter
    # run this in terminal id issue occurred related to libpq: "sudo ln -s /usr/lib/libpq.5.4.dylib /usr/lib/libpq.5.dylib"
    x = ETLRun(max_workers=None, config_user_id="postgres", config_pw="postgres", config_db="config_db", host="localhost", port=5432)
    x.main()
