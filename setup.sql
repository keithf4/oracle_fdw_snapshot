CREATE SCHEMA ofdw;

CREATE TABLE ofdw.oracle_tbltranslation (
    src_table text CONSTRAINT pk_oracle_tbltranslation PRIMARY KEY, 
    dest_table text UNIQUE NOT NULL, 
    fdw_table text UNIQUE NOT NULL
);
