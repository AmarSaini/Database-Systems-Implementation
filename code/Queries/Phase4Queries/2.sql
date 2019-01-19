SELECT SUM(c_acctbal), c_name 
FROM orders, customer
WHERE c_custkey = o_custkey AND o_totalprice < 100.0
GROUP BY c_name

