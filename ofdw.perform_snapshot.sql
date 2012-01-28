CREATE OR REPLACE FUNCTION ofdw.perform_snapshot(text) RETURNS character varying
    LANGUAGE plpgsql SECURITY DEFINER
    AS $_$
DECLARE
    v_create_sql        text;
    v_dst_tblname       text;
    v_exists            int;
    v_src_tblname       ALIAS FOR $1;
    v_fdw_tblname       text;
    v_insert_sql        text;
    v_job_name          text := 'snap : ';
    v_parts             record;
    v_rowcount          int;
    v_snap_suffix       text;  
    v_table_exists      int;
    v_view_definition   text;
    
BEGIN

v_job_name := v_job_name || v_src_tblname || ' FDW';

-- Take advisory lock to prevent multiple calls to snapshot the same table causing a deadlock
PERFORM pg_advisory_lock(hashtext('perform_snapshot'), hashtext(v_job_name));
  
SELECT dest_table, fdw_table INTO v_dst_tblname, v_fdw_tblname FROM ofdw.oracle_tbltranslation WHERE src_table = v_src_tblname;
IF v_dst_tblname IS NULL THEN
    RAISE EXCEPTION 'This table is not set up for snapshot replication: %', v_src_tblname;
END IF;

SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = v_dst_tblname;
v_exists := strpos(v_view_definition, 'snap1');
IF v_exists > 0 THEN
    v_snap_suffix := 'snap2';
ELSE
    v_snap_suffix := 'snap1';
END IF;

SELECT string_to_array(v_dst_tblname, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2] || '_' || v_snap_suffix;
IF v_table_exists = 0 THEN
    v_create_sql := 'CREATE TABLE ' || v_dst_tblname || '_' || v_snap_suffix || ' AS SELECT * FROM ' || v_fdw_tblname;
    EXECUTE v_create_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
ELSE
    EXECUTE 'TRUNCATE TABLE ' || v_dst_tblname || '_' || v_snap_suffix;
    v_insert_sql := 'INSERT INTO ' || v_dst_tblname || '_' || v_snap_suffix || ' SELECT * FROM ' || v_fdw_tblname;
    EXECUTE v_insert_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
END IF;

IF v_rowcount IS NOT NULL THEN
    EXECUTE 'ANALYZE ' || v_dst_tblname || '_' || v_snap_suffix;

    EXECUTE 'CREATE OR REPLACE VIEW ' || v_dst_tblname || ' AS ' ||
        'SELECT * FROM ' || v_dst_tblname || '_' || v_snap_suffix;
ELSE
    RAISE EXCEPTION 'No rows found in source table';
END IF;

PERFORM pg_advisory_unlock(hashtext('perform_snapshot'), hashtext(v_job_name));

RETURN v_dst_tblname || '_' || v_snap_suffix;

EXCEPTION
    WHEN RAISE_EXCEPTION THEN
        PERFORM pg_advisory_unlock(hashtext('perform_snapshot'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        PERFORM pg_advisory_unlock(hashtext('perform_snapshot'), hashtext(v_job_name));
        RAISE NOTICE '%', SQLERRM;
END                                            
$_$;
