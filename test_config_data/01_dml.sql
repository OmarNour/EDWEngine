delete from edw_config.columns_mapping;
delete from edw_config.pipelines;
delete from edw_config."columns";
delete from edw_config.layer_tables;
delete from edw_config."tables";
delete from edw_config."schemas";
delete from edw_config.db_connection;
delete from edw_config.db;
delete from edw_config.db_type;
delete from edw_config.server_ips;
delete from edw_config.servers;
delete from edw_config.data_sources;
delete from edw_config.layers;
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
INSERT INTO edw_config.data_sources ( source_name, source_level, scheduled, active)
select x.source_system_name, 0 source_level, 1 scheduled, 1 active
from smx."system" x;
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
INSERT INTO edw_config."tables" (schema_id, source_id, table_name, active)
select distinct s.id schema_id, ds.id source_id, t.table_name, 1 active
from edw_config.db d, edw_config."schemas" s, smx.stg_tables t, edw_config.data_sources ds
where t.schema = ds.source_name
and s.schema_name = 'public'
and d.db_name = 'raw_db'
and d.id = s.db_id
and t.table_name <> '' ;

INSERT INTO edw_config."tables" (schema_id, table_name, active)
select distinct s.id schema_id, t.table_name, 1 active
from edw_config.db d, edw_config."schemas" s, smx.stg_tables t
where s.schema_name = 'wrk'
and d.db_name ='ods_db'
and d.id =s.db_id
and t.table_name <> '' ;

INSERT INTO edw_config."tables" (schema_id, table_name, active)
select distinct s.id schema_id, t.table_name, 1 active
from edw_config.db d, edw_config."schemas" s, smx.stg_tables t
where s.schema_name = 'stg'
and d.db_name ='ods_db'
and d.id =s.db_id
and t.table_name <> '' ;
------------------------------------------------------------------------------
INSERT INTO edw_config.layer_tables (layer_id, table_id, active)
select distinct  l.id layer_id, t.id table_id, 1 active
from edw_config."tables" t, edw_config.layers l
where exists (select 1
				from edw_config."schemas" s
				where t.schema_id = s.id
				and  (
						((s.schema_name='wrk' and l.abbrev = 'wrk') or (s.schema_name='stg' and l.abbrev = 'stg'))
						or
						(s.schema_name='public' and l.abbrev = 'src' )
				)
			);
------------------------------------------------------------------------------
--INSERT INTO edw_config.domains (domain_name) VALUES('');
------------------------------------------------------------------------------
INSERT INTO edw_config."columns" (table_id, column_name, is_pk, is_sk, is_start_date, is_end_date, scd_type, active)
select distinct
			t.table_id
			,case when t.source_id is not null then  st.column_name_in_source else st.column_name end column_name
			,case when upper(st.pk) = 'Y' then 1 else 0 end is_pk
, 0 is_sk, 0 is_start_date, 0 is_end_date, 0 scd_type, 1 active
from edw_config.tables_details     t

	join smx.stg_tables st
	on st.table_name = t.table_name

where st.key_set_name = '';
------------------------------------------------------------------------------
INSERT INTO edw_config.pipelines (src_lyr_table_id, tgt_lyr_table_id, active)
select  src.lyr_tbl_id src_lyr_table_id, tgt.lyr_tbl_id tgt_lyr_table_id, 1 active
from edw_config.layer_tables_details src, edw_config.layer_tables_details tgt
where src.table_name = tgt.table_name
and (
		(src.layer = 'src' and tgt.layer = 'wrk')
		or
		(src.layer = 'wrk' and tgt.layer = 'stg')
	);
------------------------------------------------------------------------------
INSERT INTO edw_config.columns_mapping (pipeline_id, col_seq, src_col_id, tgt_col_id)
select p.id pipeline_id, 0 col_seq, src_cd.id src_col_id, tgt_cd.id  tgt_col_id
from edw_config.pipelines p

join edw_config.layer_tables_details src_ltd
	join edw_config.columns src_cd
	on src_cd.table_id = src_ltd.table_id
on src_ltd.lyr_tbl_id = p.src_lyr_table_id

join edw_config.layer_tables_details tgt_ltd
	join edw_config.columns tgt_cd
	on tgt_cd.table_id = tgt_ltd.table_id
on tgt_ltd.lyr_tbl_id = p.tgt_lyr_table_id

where src_ltd.table_name = tgt_ltd.table_name
and  src_cd.column_name = tgt_cd.column_name
and (
		(src_ltd.layer = 'src' and tgt_ltd.layer = 'wrk')
		or
		(src_ltd.layer = 'wrk' and tgt_ltd.layer = 'stg')
	);
