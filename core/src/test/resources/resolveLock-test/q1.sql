select
  sum(C_ACCTBAL)
from
  resolveLock_test.CUSTOMER
where
  C_ACCTBAL % 2 == 0