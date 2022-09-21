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

