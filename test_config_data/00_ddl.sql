-- public.data_source_load definition

-- Drop table

-- DROP TABLE public.data_source_load;

CREATE TABLE public.data_source_load (
	batch_id int4 NULL,
	load_id text NULL,
	source_name text NULL
);


-- public.data_sources definition

-- Drop table

-- DROP TABLE public.data_sources;

CREATE TABLE public.data_sources (
	id text NOT NULL,
	source_name text NULL,
	source_level int4 NULL,
	scheduled int4 NULL,
	active int4 NULL,
	CONSTRAINT data_sources_pk PRIMARY KEY (id)
);


-- public.layers definition

-- Drop table

-- DROP TABLE public.layers;

CREATE TABLE public.layers (
	id text NOT NULL,
	layer_name text NULL,
	abbrev text NULL,
	layer_level int4 NULL,
	active int4 NULL,
	notes text NULL,
	CONSTRAINT layers_pk PRIMARY KEY (id)
);


-- public.servers definition

-- Drop table

-- DROP TABLE public.servers;

CREATE TABLE public.servers (
	id text NOT NULL,
	server_name text NULL,
	CONSTRAINT servers_pk PRIMARY KEY (id)
);


-- public.data_source_layers definition

-- Drop table

-- DROP TABLE public.data_source_layers;

CREATE TABLE public.data_source_layers (
	id text NOT NULL,
	source_id text NULL,
	layer_id text NULL,
	ds_layer_level int4 NULL,
	active int4 NULL,
	CONSTRAINT data_source_layers_pk PRIMARY KEY (id),
	CONSTRAINT data_source_layers_fk FOREIGN KEY (source_id) REFERENCES public.data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT data_source_layers_fk_1 FOREIGN KEY (layer_id) REFERENCES public.layers(id)
);


-- public.db definition

-- Drop table

-- DROP TABLE public.db;

CREATE TABLE public.db (
	id text NOT NULL,
	server_id text NULL,
	db_name text NULL,
	notes text NULL,
	CONSTRAINT db_pk PRIMARY KEY (id),
	CONSTRAINT db_fk FOREIGN KEY (server_id) REFERENCES public.servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);


-- public.server_ips definition

-- Drop table

-- DROP TABLE public.server_ips;

CREATE TABLE public.server_ips (
	id text NOT NULL,
	server_id text NULL,
	ip text NULL,
	CONSTRAINT server_ips_pk PRIMARY KEY (id),
	CONSTRAINT server_ips_fk FOREIGN KEY (server_id) REFERENCES public.servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);


-- public.all_tables definition

-- Drop table

-- DROP TABLE public.all_tables;

CREATE TABLE public.all_tables (
	id text NOT NULL,
	db_id text NULL,
	table_name text NULL,
	active int4 NULL,
	CONSTRAINT all_tables_pk PRIMARY KEY (id),
	CONSTRAINT all_tables_fk FOREIGN KEY (db_id) REFERENCES public.db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);


-- public.pipelines definition

-- Drop table

-- DROP TABLE public.pipelines;

CREATE TABLE public.pipelines (
	id text NOT NULL,
	src_table_id text NULL,
	tgt_table_id text NULL,
	active int4 NULL,
	CONSTRAINT pipelines_pk PRIMARY KEY (id),
	CONSTRAINT pipelines_fk FOREIGN KEY (src_table_id) REFERENCES public.all_tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT pipelines_fk_1 FOREIGN KEY (tgt_table_id) REFERENCES public.all_tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);


-- public.source_pipelines definition

-- Drop table

-- DROP TABLE public.source_pipelines;

CREATE TABLE public.source_pipelines (
	id text NOT NULL,
	source_layer_id text NULL,
	pipeline_id text NULL,
	source_pipeline_level int4 NULL,
	active int4 NULL,
	CONSTRAINT source_pipelines_pk PRIMARY KEY (id),
	CONSTRAINT source_pipelines_fk FOREIGN KEY (source_layer_id) REFERENCES public.data_source_layers(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT source_pipelines_fk_1 FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);


-- public.processes definition

-- Drop table

-- DROP TABLE public.processes;

CREATE TABLE public.processes (
	id text NOT NULL,
	source_pipeline_id text NULL,
	apply_type text NULL,
	process_level int4 NULL,
	active int4 NULL,
	CONSTRAINT processes_pk PRIMARY KEY (id),
	CONSTRAINT processes_fk FOREIGN KEY (source_pipeline_id) REFERENCES public.source_pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);