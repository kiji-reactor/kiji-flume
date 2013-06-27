CREATE TABLE flume_test WITH DESCRIPTION 'Flume test table.'
ROW KEY FORMAT HASH PREFIXED(2)
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main storage.' (
  MAXVERSIONS = 1,
  TTL = FOREVER,
  COMPRESSED WITH GZIP,
  FAMILY info WITH DESCRIPTION 'Basic information' (
    tweet "string" WITH DESCRIPTION 'Contents of a Tweet.'
  )
);
