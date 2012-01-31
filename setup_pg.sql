CREATE SCHEMA ofdw;

CREATE TABLE ofdw.oracle_tbltranslation (
    src_table text CONSTRAINT pk_oracle_tbltranslation PRIMARY KEY, 
    dest_table text UNIQUE NOT NULL, 
    fdw_table text UNIQUE NOT NULL
);

CREATE FOREIGN TABLE ofdw.oracle_table_columns (
    owner text, 
    table_name text,
    column_name text,
    data_type text,
    data_length numeric,
    column_id numeric
) SERVER your_server_name
OPTIONS (schema 'your_oracle_schema', table 'PG_FDW_TABLE_COLUMNS');

