SELECT n_name
FROM nation, region
WHERE n_regionkey = r_regionkey AND r_regionkey = 1;