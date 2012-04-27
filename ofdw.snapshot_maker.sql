CREATE OR REPLACE FUNCTION ofdw.snapshot_maker(text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
BEGIN
	RAISE NOTICE 'Creating FDW table and adding rows to ofdw.oracle_tbltranslation';
	PERFORM ofdw.create_oracle_fdw_table(upper($1), 'SNAP');

	RAISE NOTICE 'attempting first snapshot';
	PERFORM ofdw.perform_snapshot(upper($1));

	RAISE NOTICE 'attempting second snapshot';
	PERFORM ofdw.perform_snapshot(upper($1));

	RAISE NOTICE 'all done, dont forget to add permissions and/or scheduling';

	RETURN;
END
$_$;
