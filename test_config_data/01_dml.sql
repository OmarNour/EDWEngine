delete from edw_config.source_layer_tables;
delete from edw_config."tables";
delete from edw_config."schemas";
delete from edw_config.db_connection;
delete from edw_config.db;
delete from edw_config.db_type;
delete from edw_config.server_ips;
delete from edw_config.servers;
delete from edw_config.source_layers;
delete from edw_config.layers;
delete from edw_config.data_sources;
------------------------------------------------------------------------------
INSERT INTO edw_config.data_sources ( source_name, source_level, scheduled, active)
select x.source_system_name , 0, 1, 1 from smx."system" x;
------------------------------------------------------------------------------
INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('source data', 'src', 0, 1, '');

INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('Landing', 'wrk', 1, 1, '');

INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('staging', 'stg', 2, 1, '');

INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('BKeys & BMaps', 'bkbm', 3, 1, '');

INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('Source Complete Image', 'srci', 4, 1, '');

INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('Data Warehouse', 'dwh', 5, 1, '');

INSERT INTO edw_config.layers (layer_name, abbrev, layer_level, active, notes)
VALUES('Presentation Layer', 'pl', 6, 1, '');
------------------------------------------------------------------------------
INSERT INTO edw_config.source_layers
(source_id, layer_id, source_layer_level, active, notes)
select x.id source_id,y.id layer_id, 0 source_layer_level, 1 active, null notes
from edw_config.data_sources x, edw_config.layers y;
------------------------------------------------------------------------------
INSERT INTO edw_config.servers (server_name) VALUES('Citizen Prod');
------------------------------------------------------------------------------
INSERT INTO edw_config.server_ips (server_id, ip)
select x.id server_id, 'localhost' ip from edw_config.servers x;
------------------------------------------------------------------------------
INSERT INTO edw_config.db_type ("type") VALUES('RDBMS');
------------------------------------------------------------------------------
INSERT INTO edw_config.db (server_id, db_type_id, db_name, notes)
select s.id server_id, dt.id db_type_id, 'raw_db' db_name, null notes
from edw_config.servers s , edw_config.db_type dt
where s.server_name = 'Citizen Prod'
and dt."type" = 'RDBMS';

INSERT INTO edw_config.db (server_id, db_type_id, db_name, notes)
select s.id server_id, dt.id db_type_id, 'ods_db' db_name, null notes
from edw_config.servers s , edw_config.db_type dt
where s.server_name = 'Citizen Prod'
and dt."type" = 'RDBMS';
------------------------------------------------------------------------------
INSERT INTO edw_config.db_connection (db_id, user_name, pw, port)
select d.id db_id, 'postgres' user_name, 'postgres' pw, 5432 port
from edw_config.db d;
------------------------------------------------------------------------------
INSERT INTO edw_config."schemas" (db_id, is_tmp, schema_name, notes)
select d.id db_id, 0 is_tmp, 'public' schema_name, null notes
from edw_config.db d
where d.db_name = 'raw_db';

INSERT INTO edw_config."schemas" (db_id, is_tmp, schema_name, notes)
select d.id db_id, 1 is_tmp, 'temp' schema_name, null notes
from edw_config.db d
where d.db_name = 'ods_db';

INSERT INTO edw_config."schemas" (db_id, is_tmp, schema_name, notes)
select d.id db_id, 0 is_tmp, 'wrk' schema_name, null notes
from edw_config.db d
where d.db_name = 'ods_db';

INSERT INTO edw_config."schemas" (db_id, is_tmp, schema_name, notes)
select d.id db_id, 0 is_tmp, 'stg' schema_name, null notes
from edw_config.db d
where d.db_name = 'ods_db';
------------------------------------------------------------------------------
INSERT INTO edw_config."tables" (schema_id, table_name, active)
select distinct s.id schema_id, t.table_name, 1 active
from edw_config.db d, edw_config."schemas" s, smx.stg_tables t
where s.schema_name = 'public'
and d.db_name ='raw_db'
and d.id =s.db_id
and t.table_name <> '' ;
------------------------------------------------------------------------------
INSERT INTO edw_config."tables" (schema_id, table_name, active)
select distinct s.id schema_id, t.table_name, 1 active
from edw_config.db d, edw_config."schemas" s, smx.stg_tables t
where s.schema_name = 'wrk'
and d.db_name ='ods_db'
and d.id =s.db_id
and t.table_name <> '' ;
------------------------------------------------------------------------------
INSERT INTO edw_config."tables" (schema_id, table_name, active)
select distinct s.id schema_id, t.table_name, 1 active
from edw_config.db d, edw_config."schemas" s, smx.stg_tables t
where s.schema_name = 'stg'
and d.db_name ='ods_db'
and d.id =s.db_id
and t.table_name <> '' ;
------------------------------------------------------------------------------
INSERT INTO edw_config.source_layer_tables (source_layer_id, table_id, active)
select distinct  sl.id source_layer_id, t.id table_id, 1 active
from edw_config."tables" t

	join smx.stg_tables st
	on t.table_name = st.table_name

	join edw_config.data_sources ds
	on ds.source_name = st."schema"

	join edw_config.source_layers sl
	on sl.source_id = ds.id


where
(
	exists (select 1
					from edw_config."schemas" s
					where s.schema_name = 'wrk'
					and t.schema_id=s.id
					and exists (select 1
								from edw_config.db d
								where s.db_id = d.id
								and d.db_name='ods_db'
								and exists (select 1
											from edw_config.servers s2
											where d.server_id=s2.id
											and s2.server_name='Citizen Prod'
											)
								)
					)

	and exists (select 1 from edw_config.layers l  where l.abbrev ='wrk' and sl.layer_id=l.id)
)
or
(
	exists (select 1
					from edw_config."schemas" s
					where s.schema_name = 'public'
					and t.schema_id=s.id
					and exists (select 1
								from edw_config.db d
								where s.db_id = d.id
								and d.db_name='raw_db'
								and exists (select 1
											from edw_config.servers s2
											where d.server_id=s2.id
											and s2.server_name='Citizen Prod'
											)
								)
					)

	and exists (select 1 from edw_config.layers l  where l.abbrev ='src' and sl.layer_id=l.id)
);
------------------------------------------------------------------------------




