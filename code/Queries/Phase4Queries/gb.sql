SELECT SUM(n_nationkey), n_regionkey FROM nation WHERE n_nationkey > 0 GROUP BY n_regionkey
