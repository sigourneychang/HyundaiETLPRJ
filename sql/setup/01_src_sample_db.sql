-- Sample database on the SOURCE cluster (re-runnable).
--   ./scripts/beeline.sh src -f sql/setup/01_src_sample_db.sql
-- Covers the table shapes a Hive 2.x migration has to handle:
--   EXTERNAL  : ORC / PARQUET / TEXT / partitioned ORC
--   MANAGED   : ORC / PARQUET / TEXT / partitioned ORC   (plain CREATE TABLE = managed, non-ACID)
--   ACID      : ORC + bucketed + transactional=true      (only ACID form Hive 2 supports)
SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE DATABASE IF NOT EXISTS sample_db;

-- ---------- EXTERNAL ----------
DROP TABLE IF EXISTS sample_db.ext_orc;
CREATE EXTERNAL TABLE sample_db.ext_orc (col1 STRING, col2 STRING, col3 STRING)
STORED AS ORC LOCATION '/data/sample_db/ext_orc';

DROP TABLE IF EXISTS sample_db.ext_parquet;
CREATE EXTERNAL TABLE sample_db.ext_parquet (col1 STRING, col2 STRING, col3 STRING)
STORED AS PARQUET LOCATION '/data/sample_db/ext_parquet';

DROP TABLE IF EXISTS sample_db.ext_text;
CREATE EXTERNAL TABLE sample_db.ext_text (col1 STRING, col2 STRING, col3 STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/data/sample_db/ext_text';

DROP TABLE IF EXISTS sample_db.ext_orc_part;
CREATE EXTERNAL TABLE sample_db.ext_orc_part (col1 STRING, col2 STRING, col3 STRING)
PARTITIONED BY (dt STRING)
STORED AS ORC LOCATION '/data/sample_db/ext_orc_part';

-- ---------- MANAGED (non-ACID) ----------
DROP TABLE IF EXISTS sample_db.mng_orc;
CREATE TABLE sample_db.mng_orc (col1 STRING, col2 STRING, col3 STRING) STORED AS ORC;

DROP TABLE IF EXISTS sample_db.mng_parquet;
CREATE TABLE sample_db.mng_parquet (col1 STRING, col2 STRING, col3 STRING) STORED AS PARQUET;

DROP TABLE IF EXISTS sample_db.mng_text;
CREATE TABLE sample_db.mng_text (col1 STRING, col2 STRING, col3 STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

DROP TABLE IF EXISTS sample_db.mng_orc_part;
CREATE TABLE sample_db.mng_orc_part (col1 STRING, col2 STRING, col3 STRING)
PARTITIONED BY (dt STRING) STORED AS ORC;

-- ---------- MANAGED ACID ----------
DROP TABLE IF EXISTS sample_db.acid_orc;
CREATE TABLE sample_db.acid_orc (col1 STRING, col2 STRING, col3 STRING)
CLUSTERED BY (col1) INTO 2 BUCKETS
STORED AS ORC TBLPROPERTIES ('transactional'='true');

-- ---------- DATA ----------
INSERT OVERWRITE TABLE sample_db.ext_orc VALUES
  ('eo_a1','eo_b1','eo_c1'),('eo_a2','eo_b2','eo_c2'),('eo_a3','eo_b3','eo_c3'),('eo_a4','eo_b4','eo_c4'),('eo_a5','eo_b5','eo_c5');
INSERT OVERWRITE TABLE sample_db.ext_parquet VALUES
  ('ep_a1','ep_b1','ep_c1'),('ep_a2','ep_b2','ep_c2'),('ep_a3','ep_b3','ep_c3'),('ep_a4','ep_b4','ep_c4'),('ep_a5','ep_b5','ep_c5');
INSERT OVERWRITE TABLE sample_db.ext_text VALUES
  ('et_a1','et_b1','et_c1'),('et_a2','et_b2','et_c2'),('et_a3','et_b3','et_c3'),('et_a4','et_b4','et_c4'),('et_a5','et_b5','et_c5');
INSERT OVERWRITE TABLE sample_db.ext_orc_part PARTITION (dt) VALUES
  ('ep1_a1','ep1_b1','ep1_c1','2026-11-01'),('ep1_a2','ep1_b2','ep1_c2','2026-11-01'),
  ('ep2_a1','ep2_b1','ep2_c1','2026-11-02'),('ep2_a2','ep2_b2','ep2_c2','2026-11-02'),('ep2_a3','ep2_b3','ep2_c3','2026-11-02');
INSERT OVERWRITE TABLE sample_db.mng_orc VALUES
  ('mo_a1','mo_b1','mo_c1'),('mo_a2','mo_b2','mo_c2'),('mo_a3','mo_b3','mo_c3'),('mo_a4','mo_b4','mo_c4'),('mo_a5','mo_b5','mo_c5');
INSERT OVERWRITE TABLE sample_db.mng_parquet VALUES
  ('mp_a1','mp_b1','mp_c1'),('mp_a2','mp_b2','mp_c2'),('mp_a3','mp_b3','mp_c3'),('mp_a4','mp_b4','mp_c4'),('mp_a5','mp_b5','mp_c5');
INSERT OVERWRITE TABLE sample_db.mng_text VALUES
  ('mt_a1','mt_b1','mt_c1'),('mt_a2','mt_b2','mt_c2'),('mt_a3','mt_b3','mt_c3'),('mt_a4','mt_b4','mt_c4'),('mt_a5','mt_b5','mt_c5');
INSERT OVERWRITE TABLE sample_db.mng_orc_part PARTITION (dt) VALUES
  ('mp1_a1','mp1_b1','mp1_c1','2026-11-01'),('mp1_a2','mp1_b2','mp1_c2','2026-11-01'),
  ('mp2_a1','mp2_b1','mp2_c1','2026-11-02'),('mp2_a2','mp2_b2','mp2_c2','2026-11-02'),('mp2_a3','mp2_b3','mp2_c3','2026-11-02');
INSERT INTO TABLE sample_db.acid_orc VALUES
  ('ac_a1','ac_b1','ac_c1'),('ac_a2','ac_b2','ac_c2'),('ac_a3','ac_b3','ac_c3'),('ac_a4','ac_b4','ac_c4'),('ac_a5','ac_b5','ac_c5');

-- ---------- VIEW (SHOW TABLES lists views too; no data to copy) ----------
DROP VIEW IF EXISTS sample_db.v_ext_orc;
CREATE VIEW sample_db.v_ext_orc AS SELECT col1, col2 FROM sample_db.ext_orc WHERE col1 <> 'eo_a1';

-- ---------- CHECK ----------
SELECT 'ext_orc' tbl, count(*) cnt FROM sample_db.ext_orc
UNION ALL SELECT 'ext_parquet',  count(*) FROM sample_db.ext_parquet
UNION ALL SELECT 'ext_text',     count(*) FROM sample_db.ext_text
UNION ALL SELECT 'ext_orc_part', count(*) FROM sample_db.ext_orc_part
UNION ALL SELECT 'mng_orc',      count(*) FROM sample_db.mng_orc
UNION ALL SELECT 'mng_parquet',  count(*) FROM sample_db.mng_parquet
UNION ALL SELECT 'mng_text',     count(*) FROM sample_db.mng_text
UNION ALL SELECT 'mng_orc_part', count(*) FROM sample_db.mng_orc_part
UNION ALL SELECT 'acid_orc',     count(*) FROM sample_db.acid_orc;
