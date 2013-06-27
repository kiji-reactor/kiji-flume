CREATE TABLE tweets WITH DESCRIPTION 'Contains tweets from Twitter.'
ROW KEY FORMAT HASH PREFIXED(2)
WITH LOCALITY GROUP default WITH DESCRIPTION 'Main storage.' (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  COMPRESSED WITH GZIP,
  FAMILY tweet WITH DESCRIPTION 'Information about a tweet' (
    text "string" WITH DESCRIPTION 'Content of a tweet.'
  )
);
