select
  sum(C_ACCTBAL)
from
  resolveLock_test.CUSTOMER

UNION

select
  avg(C_ACCTBAL)
from
  resolveLock_test.CUSTOMER

UNION

select
  count(*)
from
  resolveLock_test.CUSTOMER
GROUP BY
  C_NATIONKEY

UNION

select
  C_CUSTKEY + C_NATIONKEY
from
  resolveLock_test.CUSTOMER

UNION

select
  count(*)
from
  resolveLock_test.CUSTOMER

UNION

select
  count(*)
from
  resolveLock_test.CUSTOMER
GROUP BY
  C_NATIONKEY