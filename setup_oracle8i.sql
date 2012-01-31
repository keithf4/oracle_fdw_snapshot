CREATE VIEW PG_FDW_TABLE_COLUMNS AS select owner, table_name, column_name, data_type, data_length, column_id from dba_tab_columns;
