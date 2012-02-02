CREATE OR REPLACE VIEW PG_FDW_TABLE_COLUMNS AS SELECT owner, table_name, column_name, data_type, data_length, column_id FROM all_tab_columns;
