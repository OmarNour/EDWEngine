set search_path = 'edw_config';

-- Drop table
DROP TABLE if exists processes;
DROP TABLE if exists source_pipelines;
DROP TABLE if exists pipelines;
DROP TABLE if exists data_source_layers;
DROP TABLE if exists layers;
DROP TABLE if exists data_sources;
DROP TABLE if exists db_connection;
DROP TABLE if exists all_tables;
DROP TABLE if exists db;
DROP TABLE if exists server_ips;
DROP TABLE if exists db_type;
DROP TABLE if exists servers;
DROP TABLE if exists data_source_load;

CREATE TABLE data_sources (
	id text NOT NULL,
	source_name text NULL,
	source_level int4 NULL,
	scheduled int4 NULL,
	active int4 NULL,
	CONSTRAINT data_sources_pk PRIMARY KEY (id)
);

CREATE TABLE db_type (
	id int4 NOT NULL,
	"type" varchar NULL,
	CONSTRAINT db_type_pkey PRIMARY KEY (id)
);

CREATE TABLE layers (
	id text NOT NULL,
	layer_name text NULL,
	abbrev text NULL,
	layer_level int4 NULL,
	active int4 NULL,
	notes text NULL,
	CONSTRAINT layers_pk PRIMARY KEY (id)
);

CREATE TABLE servers (
	id text NOT NULL,
	server_name text NULL,
	CONSTRAINT servers_pk PRIMARY KEY (id)
);

CREATE TABLE data_source_layers (
	id text NOT NULL,
	source_id text NULL,
	layer_id text NULL,
	ds_layer_level int4 NULL,
	active int4 NULL,
	CONSTRAINT data_source_layers_pk PRIMARY KEY (id),
	CONSTRAINT data_source_layers_fk FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT data_source_layers_fk_1 FOREIGN KEY (layer_id) REFERENCES layers(id)
);

CREATE TABLE data_source_load (
	batch_id int4 NULL,
	source_id varchar NULL,
	load_id text NULL,
	CONSTRAINT data_source_load_fk FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE db (
	id text NOT NULL,
	server_id text NULL,
	db_name text NULL,
	notes text NULL,
	db_type_id int4 NULL,
	CONSTRAINT db_pk PRIMARY KEY (id),
	CONSTRAINT db_fk FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT db_fk1 FOREIGN KEY (db_type_id) REFERENCES db_type(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE db_connection (
	id text NOT NULL,
	db_id text NULL,
	shcema text NULL,
	user_name text NULL,
	pw text NULL,
	port text NULL,
	CONSTRAINT db_connection_pk PRIMARY KEY (id),
	CONSTRAINT db_connection_fk FOREIGN KEY (db_id) REFERENCES db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE server_ips (
	id text NOT NULL,
	server_id text NULL,
	ip text NULL,
	CONSTRAINT server_ips_pk PRIMARY KEY (id),
	CONSTRAINT server_ips_fk FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE all_tables (
	id text NOT NULL,
	db_id text NULL,
	table_name text NULL,
	active int4 NULL,
	CONSTRAINT all_tables_pk PRIMARY KEY (id),
	CONSTRAINT all_tables_fk FOREIGN KEY (db_id) REFERENCES db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE pipelines (
	id text NOT NULL,
	src_table_id text NULL,
	tgt_table_id text NULL,
	active int4 NULL,
	CONSTRAINT pipelines_pk PRIMARY KEY (id),
	CONSTRAINT pipelines_fk FOREIGN KEY (src_table_id) REFERENCES all_tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT pipelines_fk_1 FOREIGN KEY (tgt_table_id) REFERENCES all_tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE source_pipelines (
	id text NOT NULL,
	source_layer_id text NULL,
	pipeline_id text NULL,
	source_pipeline_level int4 NULL,
	active int4 NULL,
	CONSTRAINT source_pipelines_pk PRIMARY KEY (id),
	CONSTRAINT source_pipelines_fk FOREIGN KEY (source_layer_id) REFERENCES data_source_layers(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT source_pipelines_fk_1 FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE processes (
	id text NOT NULL,
	source_pipeline_id text NULL,
	apply_type text NULL,
	process_level int4 NULL,
	active int4 NULL,
	CONSTRAINT processes_pk PRIMARY KEY (id),
	CONSTRAINT processes_fk FOREIGN KEY (source_pipeline_id) REFERENCES source_pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);