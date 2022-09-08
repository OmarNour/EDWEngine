set search_path = 'edw_config';

-- Drop table
-- DROP TABLE if exists processes;
-- DROP TABLE if exists layer_pipelines;
DROP TABLE if exists columns;
DROP TABLE if exists pipelines;
-- DROP TABLE if exists data_source_layers;
DROP TABLE if exists layers;
DROP TABLE if exists db_connection;
DROP TABLE if exists tables;
DROP TABLE if exists schemas;
DROP TABLE if exists db;
DROP TABLE if exists server_ips;
DROP TABLE if exists db_type;
DROP TABLE if exists servers;
DROP TABLE if exists data_source_load;
DROP TABLE if exists data_sources;

CREATE table if not exists data_sources (
	id int4 NOT NULL,
	source_name varchar not NULL,
	source_level int4 not NULL,
	scheduled int4 not NULL,
	active int4 not NULL,
	CONSTRAINT data_sources_pk PRIMARY KEY (id)
);

CREATE table if not exists data_source_load (
    id int4 NOT NULL,
	batch_id int4 not NULL,
	source_id int4 not NULL,
	load_id varchar not NULL,
	processed   int4 not null,
	CONSTRAINT data_source_load_pk PRIMARY KEY (id),
	CONSTRAINT data_source_load_fk FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists servers (
	id int4 NOT NULL,
	server_name varchar not NULL,
	CONSTRAINT servers_pk PRIMARY KEY (id)
);

CREATE table if not exists db_type (
	id int4 NOT NULL,
	"type" varchar not NULL,
	CONSTRAINT db_type_pkey PRIMARY KEY (id)
);

CREATE table if not exists server_ips (
	id int4 NOT NULL,
	server_id int4 not NULL,
	ip varchar not NULL,
	CONSTRAINT server_ips_pk PRIMARY KEY (id),
	CONSTRAINT server_ips_fk FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists db (
	id int4 NOT NULL,
	server_id int4 not NULL,
	db_type_id int4 not NULL,
	db_name varchar not NULL,
	notes text NULL,
	CONSTRAINT db_pk PRIMARY KEY (id),
	CONSTRAINT db_fk FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT db_fk1 FOREIGN KEY (db_type_id) REFERENCES db_type(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists db_connection (
	id int4 NOT NULL,
	db_id int4 not NULL,
	user_name varchar not NULL,
	pw varchar not NULL,
	port varchar NULL,
	CONSTRAINT db_connection_pk PRIMARY KEY (id),
	CONSTRAINT db_connection_fk FOREIGN KEY (db_id) REFERENCES db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists schemas (
	id int4 NOT NULL,
	db_id int4 not NULL,
	schema_name varchar not NULL,
	notes text NULL,
	CONSTRAINT schemas_pk PRIMARY KEY (id),
	CONSTRAINT schemas_fk FOREIGN KEY (db_id) REFERENCES db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists tables (
	id int4 NOT NULL,
	schema_id int4 not NULL,
	source_id int4 null,
	table_name varchar not NULL,
	active int4 not NULL,
	CONSTRAINT tables_pk PRIMARY KEY (id),
	CONSTRAINT tables_fk1 FOREIGN KEY (schema_id) REFERENCES schemas(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT tables_fk2 FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE layers (
	id int4 NOT NULL,
	layer_name varchar not NULL,
	abbrev varchar NULL,
	layer_level int4 NULL,
	active int4 NULL,
	notes text NULL,
	CONSTRAINT layers_pk PRIMARY KEY (id)
);

CREATE table if not exists pipelines (
	id int4 NOT NULL,
	src_table_id int4 not NULL,
	tgt_table_id int4 not NULL,
	layer_id int4 not NULL,
	active int4 NULL,
	CONSTRAINT pipelines_pk PRIMARY KEY (id),
	CONSTRAINT pipelines_fk FOREIGN KEY (src_table_id) REFERENCES tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT pipelines_fk_1 FOREIGN KEY (tgt_table_id) REFERENCES tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT pipelines_fk2 FOREIGN KEY (layer_id) REFERENCES layers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists columns (
	id int4 NOT NULL,
	table_id int4 not NULL,
	column_name varchar not NULL,
-- 	data_type   varchar not NULL,
	active int4 not NULL,
	CONSTRAINT columns_pk PRIMARY KEY (id),
	CONSTRAINT columns_fk1 FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

-- CREATE table if not exists data_source_layers (
-- 	id int4 NOT NULL,
-- 	source_id int4 not NULL,
-- 	layer_id int4 not NULL,
-- 	ds_layer_level int4 not NULL,
-- 	active int4 not NULL,
-- 	CONSTRAINT data_source_layers_pk PRIMARY KEY (id),
-- 	CONSTRAINT data_source_layers_fk FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
-- 	CONSTRAINT data_source_layers_fk_1 FOREIGN KEY (layer_id) REFERENCES layers(id)
-- );

-- CREATE table if not exists layer_pipelines (
-- 	id int4 NOT NULL,
-- 	layer_id int4 not NULL,
-- 	pipeline_id int4 not NULL,
-- 	layer_pipeline_level int4 not NULL,
-- 	active int4 NULL,
-- 	CONSTRAINT layer_pipelines_pk PRIMARY KEY (id),
-- 	CONSTRAINT layer_pipelines_fk FOREIGN KEY (layer_id) REFERENCES layers(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
-- 	CONSTRAINT layer_pipelines_fk_1 FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT
-- );
-------------------

-- CREATE table if not exists processes (
-- 	id int4 NOT NULL,
-- 	source_pipeline_id int4 not NULL,
-- 	apply_type text NULL,
-- 	process_level int4 NULL,
-- 	active int4 NULL,
-- 	CONSTRAINT processes_pk PRIMARY KEY (id),
-- 	CONSTRAINT processes_fk FOREIGN KEY (source_pipeline_id) REFERENCES source_pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT
-- );
--