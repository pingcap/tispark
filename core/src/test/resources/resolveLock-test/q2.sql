select
  sum(C_ACCTBAL)
from
  tidb_resolveLock_test.CUSTOMER

UNION

select
  avg(C_ACCTBAL)
from
  tidb_resolveLock_test.CUSTOMER

UNION

select
  count(*)
from
  tidb_resolveLock_test.CUSTOMER
GROUP BY
  C_NATIONKEY

UNION

select
  C_CUSTKEY + C_NATIONKEY
from
  tidb_resolveLock_test.CUSTOMER

UNION

select
  count(*)
from
  tidb_resolveLock_test.CUSTOMER

UNION

select
  count(*)
from
  tidb_resolveLock_test.CUSTOMER
GROUP BY
  C_NATIONKEY