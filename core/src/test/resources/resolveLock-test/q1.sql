select
  sum(C_ACCTBAL)
from
  CUSTOMER
where
  C_ACCTBAL % 2 == 0