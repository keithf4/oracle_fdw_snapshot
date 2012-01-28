CREATE OR REPLACE FUNCTION ofdw.snapshot_destroyer(text, text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
    
DECLARE
    v_dest_table        text;
    v_exists            int;
    v_fdw_table         text;
    v_snap_suffix       text;
    v_sql               text;
    v_src_table         text;
    v_view_definition   text;
    
BEGIN

SELECT src_table, dest_table, fdw_table INTO v_src_table, v_dest_table, v_fdw_table
    FROM ofdw.oracle_tbltranslation WHERE src_table = $1;
IF v_dest_table IS NULL THEN
    RAISE EXCEPTION 'This table is not set up for snapshot replication: %', v_src_table;
END IF;

-- Make a brand new, real table to keep the data that is not part of the snap system anymore
IF $2 = 'ARCHIVE' THEN

    SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = v_dest_table;
    v_exists := strpos(v_view_definition, 'snap1');
    IF v_exists > 0 THEN
        v_snap_suffix := 'snap1';
    ELSE
        v_snap_suffix := 'snap2';
    END IF;
    
    EXECUTE 'DROP VIEW ' || v_dest_table;
    EXECUTE 'CREATE TEMPORARY TABLE tmp_snapshot_destroy AS SELECT * FROM ' || v_dest_table || '_' || v_snap_suffix;
    EXECUTE 'CREATE TABLE ' || v_dest_table || ' AS SELECT * FROM tmp_snapshot_destroy';
    
ELSE

    EXECUTE 'DROP VIEW ' || v_dest_table;    

END IF;

EXECUTE 'DROP TABLE ' || v_dest_table || '_snap1';
EXECUTE 'DROP TABLE ' || v_dest_table || '_snap2';

EXECUTE 'DELETE FROM ofdw.oracle_tbltranslation WHERE src_table = ' || quote_literal(v_src_table);
    
EXECUTE 'DROP FOREIGN TABLE ' || v_fdw_table;

END
$_$;
