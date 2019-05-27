-- tabix tab 1
CREATE DATABASE merge_tree;

-- combination 1
CREATE TABLE merge_tree.event_time_batch
(
  id UInt64,
  time DateTime,
  type UInt16,
  pokemon_id UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time, id);
-- Pay attention to order of fields, and please don't use such terrible order by fields
-- We're running pokemon business, so we're kind of allowed to use a bit of magic.

CREATE TABLE merge_tree.event_time_single
(
  id UInt64,
  time DateTime,
  type UInt16,
  pokemon_id UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time, id);

-- combination 2
CREATE TABLE merge_tree.event_time_order_func_batch
(
  id UInt64,
  time DateTime,
  type UInt16,
  pokemon_id UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (toYYYYMM(time), id);

CREATE TABLE merge_tree.event_time_order_func_single
(
  id UInt64,
  time DateTime,
  type UInt16,
  pokemon_id UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (toYYYYMM(time), id);

-- combination 3
CREATE TABLE merge_tree.event_date_batch
(
  id UInt64,
  time DateTime,
  date Date,
  type UInt16,
  pokemon_id UInt16
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY (date, id);

CREATE TABLE merge_tree.event_date_single
(
  id UInt64,
  time DateTime,
  date Date,
  type UInt16,
  pokemon_id UInt16
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY (date, id);

------
SELECT table, formatReadableSize(size) as size, rows FROM (
    SELECT
        table,
        sum(bytes) AS size,
        sum(rows) AS rows,
        min(min_date) AS min_date,
        max(max_date) AS max_date
    FROM system.parts
    WHERE active
    GROUP BY table
    ORDER BY rows DESC
)