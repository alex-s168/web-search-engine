CREATE DATABASE IF NOT EXISTS pages;

CREATE TABLE IF NOT EXISTS pages.scraped
(
    url   String   NOT NULL PRIMARY KEY,
    time  DateTime NOT NULL,
    title String   NULL
)
ENGINE = MergeTree()
;

CREATE TABLE IF NOT EXISTS pages.words
(
    word String NOT NULL PRIMARY KEY,
    url  String NOT NULL PRIMARY KEY,
    num  UInt64 NOT NULL
)
ENGINE = MergeTree()
ORDER BY (word, url)
PARTITION BY substr(word, 1, 1)
;

CREATE TABLE IF NOT EXISTS pages.to_query
(
    url String NOT NULL PRIMARY KEY
)
ENGINE = SummingMergeTree()
ORDER BY url
;

CREATE TABLE IF NOT EXISTS pages.force_query
(
    url String NOT NULL PRIMARY KEY
)
ENGINE = MergeTree()
;

CREATE TABLE IF NOT EXISTS pages.links
(
    from_url String NOT NULL,
    to_url   String NOT NULL PRIMARY KEY
)
ENGINE = MergeTree()
;
  
CREATE MATERIALIZED VIEW IF NOT EXISTS pages.num_links
ENGINE = SummingMergeTree()
ORDER BY url
POPULATE
AS SELECT
    to_url AS url,
    COUNT(to_url) AS val
FROM pages.links
GROUP BY to_url;

CREATE MATERIALIZED VIEW IF NOT EXISTS pages.num_words
ENGINE = SummingMergeTree()
ORDER BY word
POPULATE
AS
SELECT
    word,
    SUM(num) AS total_num
FROM pages.words
GROUP BY word;

