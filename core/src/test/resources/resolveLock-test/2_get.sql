update
  resolveLock_test.CUSTOMER
set
  C_ACCTBAL
    = C_ACCTBAL + $1
where
  C_CUSTKEY
    = $2