CREATE OR REPLACE FUNCTION ofdw.create_oracle_fdw_table(text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_check             int;
    v_coldef            text := '';
    v_create_sql        text;
    v_dest_schema       text;
    v_dest_table        text;
    v_fdw_table         text;
    v_insert_sql        text;
    v_src_schema        text;
    v_src_table         text;
    v_tbldef            record;  
    
BEGIN

FOR v_tbldef IN 
    SELECT owner, table_name, column_name,
        CASE WHEN data_type = 'VARCHAR' THEN 'text' 
        WHEN data_type = 'VARCHAR2' THEN 'text'
        WHEN data_type = 'NVARCHAR2' THEN 'text'  
        WHEN data_type = 'CHAR' THEN 'char'||'('||data_length||')'
        WHEN data_type = 'NCHAR' THEN 'char'||'('||data_length||')'
        WHEN data_type = 'CLOB' THEN 'text'
        WHEN data_type = 'CFILE' THEN 'text'
        WHEN data_type = 'RAW' THEN 'bytea'
        WHEN data_type = 'BLOB' THEN 'bytea'
        WHEN data_type = 'BFILE' THEN 'bytea'
        WHEN data_type = 'NUMBER' THEN 'numeric'
        WHEN data_type = 'FLOAT' THEN 'float8'
        WHEN data_type = 'BINARY_FLOAT' THEN 'float8'
        WHEN data_type = 'DATE' THEN 'timestamp'
        WHEN data_type = 'TIMESTAMP' THEN 'timestamp'
        WHEN data_type = 'TIMESTAMP WITH TIME ZONE' THEN 'timestamptz'
        WHEN data_type = 'INTERVAL YEAR TO MONTH' THEN 'interval'
        WHEN data_type = 'INTERVAL DAY TO SECOND' THEN 'interval'
        ELSE 'WTF->' || data_type END AS data_type 
        FROM ofdw.oracle_table_columns
        WHERE owner || '.' || table_name = $1
        ORDER BY column_id ASC LOOP

    IF v_tbldef.data_type ~ 'WTF' THEN
        RAISE EXCEPTION 'Unknown data type mapping (%)',v_tbldef.data_type;
    END IF;
    
    v_src_schema := upper(v_tbldef.owner);
    v_src_table := upper(v_tbldef.table_name);
    
    v_dest_schema := lower(v_tbldef.owner);
    v_dest_table :=  lower(v_tbldef.table_name);
    
    IF v_coldef <> '' THEN 
        v_coldef := v_coldef || ', ';
    END IF;
    
    v_coldef := v_coldef || v_tbldef.column_name || ' ' || v_tbldef.data_type; 
    
END LOOP;

IF v_dest_schema IS NULL OR v_dest_table IS NULL THEN
    RAISE EXCEPTION 'Source table does not exist (%)', $1;
END IF;
  
EXECUTE 'SELECT count(1) FROM pg_namespace WHERE nspname = $1'
    INTO v_check
    USING v_dest_schema;
    
IF v_check = 0 THEN
    RAISE EXCEPTION 'Destination schema % does not exist', v_dest_schema;
END IF;

v_fdw_table := 'ofdw.' || v_dest_schema || '_' || v_dest_table;

v_create_sql := 'CREATE FOREIGN TABLE '|| v_fdw_table || '(' || v_coldef || ') SERVER flpdt_server ' ||
    'OPTIONS (schema ' || quote_literal(v_src_schema) || ', table ' || quote_literal(v_src_table) || ')';
    
RAISE NOTICE 'create sql: %', v_create_sql;

EXECUTE v_create_sql;

v_src_table := v_src_schema || '.' || v_src_table;
v_dest_table := v_dest_schema || '.' || v_dest_table;
  
v_insert_sql := 'INSERT INTO ofdw.oracle_tbltranslation (src_table, dest_table, fdw_table) VALUES (' ||
    quote_literal(v_src_table) || ', ' || quote_literal(v_dest_table) || ', ' || quote_literal(v_fdw_table) || ')';
    
EXECUTE v_insert_sql;

END
$_$;
