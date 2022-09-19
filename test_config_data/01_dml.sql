INSERT INTO data_sources (id, source_name, source_level, scheduled, active) VALUES(0, 'Retail_branch_a', 0, 1, 1);

INSERT INTO layers (id, layer_name, abbrev, layer_level, active, notes) VALUES(0, 'Raw Data', 'RawDB', 0, 1, '');
INSERT INTO layers (id, layer_name, abbrev, layer_level, active, notes) VALUES(1, 'Landing DB', 'LandingDB', 1, 1, '');
INSERT INTO layers (id, layer_name, abbrev, layer_level, active, notes) VALUES(2, 'Operational DB', 'ODS', 2, 1, '');
INSERT INTO layers (id, layer_name, abbrev, layer_level, active, notes) VALUES(3, 'Surrogate Keys', 'SKeys', 3, 1, '');
INSERT INTO layers (id, layer_name, abbrev, layer_level, active, notes) VALUES(4, 'ODS + SKeys', 'SRCI', 1, 4, '');
INSERT INTO layers (id, layer_name, abbrev, layer_level, active, notes) VALUES(5, 'Data Warehouse', 'EDWH', 5, 1, '');

INSERT INTO source_layers (id, source_id, layer_id, source_layer_level, active, notes) VALUES(0, 0, 0, 0, 1, '');
