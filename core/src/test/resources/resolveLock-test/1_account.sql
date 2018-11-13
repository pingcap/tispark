select
  C_ACCTBAL
from
  resolveLock_test.CUSTOMER
where
  C_CUSTKEY
    = $1