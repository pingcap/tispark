select
  sum(C_ACCTBAL)
from
  tidb_resolveLock_test.CUSTOMER
where
  C_ACCTBAL % 2 == 0