set search_path = 'edw_config';

-- Drop table
-- DROP TABLE if exists processes;
-- DROP TABLE if exists layer_pipelines;
DROP TABLE if exists columns_mapping;
DROP TABLE if exists columns;
DROP TABLE if exists pipelines;
DROP TABLE if exists layer_tables;
DROP TABLE if exists db_connection;
DROP TABLE if exists tables;
DROP TABLE if exists schemas;
DROP TABLE if exists db;
DROP TABLE if exists server_ips;
DROP TABLE if exists db_type;
DROP TABLE if exists servers;
DROP TABLE if exists data_source_load;
-- DROP TABLE if exists source_layers;
DROP TABLE if exists data_sources;
DROP TABLE if exists layers;

CREATE table if not exists data_sources (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	source_name varchar not NULL,
	source_level int4 not NULL,
	scheduled int4 not NULL,
	active int4 not NULL default 1,
	CONSTRAINT data_sources_pk PRIMARY KEY (id)
);

-- CREATE table if not exists source_layers (
-- 	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
-- 	source_id int4 NOT NULL,
-- 	layer_id int4 NOT NULL,
-- 	source_layer_level int4 NULL,
-- 	active int4 NULL default 1,
-- 	notes text NULL,
-- 	CONSTRAINT source_layers_pk PRIMARY KEY (id),
-- 	CONSTRAINT source_layers_fk1 FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
-- 	CONSTRAINT source_layers_fk2 FOREIGN KEY (layer_id) REFERENCES layers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
-- );

CREATE table if not exists data_source_load (
    id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	batch_id int4 not NULL,
	source_id int4 not NULL,
	load_id varchar not NULL,
	processed   int4 not null,
	CONSTRAINT data_source_load_pk PRIMARY KEY (id),
	CONSTRAINT data_source_load_fk FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists servers (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	server_name varchar not NULL,
	CONSTRAINT servers_pk PRIMARY KEY (id)
);

CREATE table if not exists db_type (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	"type" varchar not NULL,
	CONSTRAINT db_type_pkey PRIMARY KEY (id)
);

CREATE table if not exists server_ips (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	server_id int4 not NULL,
	ip varchar not NULL,
	CONSTRAINT server_ips_pk PRIMARY KEY (id),
	CONSTRAINT server_ips_fk FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists db (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	server_id int4 not NULL,
	db_type_id int4 not NULL,
	db_name varchar not NULL,
	notes text NULL,
	CONSTRAINT db_pk PRIMARY KEY (id),
	CONSTRAINT db_fk FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT db_fk1 FOREIGN KEY (db_type_id) REFERENCES db_type(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists db_connection (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	db_id int4 not NULL,
	user_name varchar not NULL,
	pw varchar not NULL,
	port varchar NULL,
	CONSTRAINT db_connection_pk PRIMARY KEY (id),
	CONSTRAINT db_connection_fk FOREIGN KEY (db_id) REFERENCES db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists schemas (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	db_id int4 not NULL,
	is_tmp int2 not NULL default 0,
	schema_name varchar not NULL,
	notes text NULL,
	CONSTRAINT schemas_pk PRIMARY KEY (id),
	CONSTRAINT schemas_fk FOREIGN KEY (db_id) REFERENCES db(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists tables (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	schema_id int4 not NULL,
	source_id int4,
	table_name varchar not NULL,
	active int4 not NULL default 1,
	CONSTRAINT tables_pk PRIMARY KEY (id),
	CONSTRAINT tables_fk1 FOREIGN KEY (schema_id) REFERENCES schemas(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT tables_fk2 FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists domains (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	domain_name varchar not NULL,
	CONSTRAINT domains_pk PRIMARY KEY (id)
);

CREATE table if not exists columns (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	table_id int4 not NULL,
	column_name varchar not NULL,
	is_pk           int2    not NULL default 0,
	is_sk           int2    not NULL default 0,
	is_start_date   int2    not NULL default 0,
	is_end_date     int2    not NULL default 0,
	scd_type        int2    not null default 1, -- 0 ignore, 1 overwrite, 2 history
	domain_id       int4    null,
-- 	data_type   varchar not NULL,
	active int4 not NULL default 1,
	CONSTRAINT columns_pk PRIMARY KEY (id),
	CONSTRAINT columns_fk1 FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT columns_fk2 FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists layers (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	layer_name varchar not NULL,
	abbrev varchar NULL,
	layer_level int4 NULL,
	active int4 NULL default 1,
	notes text NULL,
	CONSTRAINT layers_pk PRIMARY KEY (id)
);

CREATE table if not exists layer_tables (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	layer_id int4 not NULL,
	table_id int4 not NULL,
	active int4 not NULL default 1,
	CONSTRAINT layer_tables_pk PRIMARY KEY (id),
	CONSTRAINT layer_tables_fk1 FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT layer_tables_fk2 FOREIGN KEY (layer_id) REFERENCES layers(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists pipelines (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	src_lyr_table_id int4 not NULL,
	tgt_lyr_table_id int4 not NULL,
	active int4 NULL default 1,
	CONSTRAINT pipelines_pk PRIMARY KEY (id),
	CONSTRAINT pipelines_fk FOREIGN KEY (src_lyr_table_id) REFERENCES layer_tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT pipelines_fk_1 FOREIGN KEY (tgt_lyr_table_id) REFERENCES layer_tables(id) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE table if not exists columns_mapping (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	pipeline_id int4 not null,
	col_seq int4 not null,
	tgt_col_id  int4 not null,
	src_col_id  int4 not null,
	CONSTRAINT columns_mapping_pk PRIMARY KEY (id),
	CONSTRAINT columns_mapping_fk  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT columns_mapping_fk1 FOREIGN KEY (tgt_col_id) REFERENCES columns(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
	CONSTRAINT columns_mapping_fk2 FOREIGN KEY (src_col_id) REFERENCES columns(id) ON DELETE RESTRICT ON UPDATE RESTRICT

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