select
  sum(C_ACCTBAL)
from
  CUSTOMER

UNION

select
  avg(C_ACCTBAL)
from
  CUSTOMER

UNION

select
  count(*)
from
  CUSTOMER
GROUP BY
  C_NATIONKEY

UNION

select
  C_CUSTKEY + C_NATIONKEY
from
  CUSTOMER

UNION

select
  count(*)
from
  CUSTOMER

UNION

select
  count(*)
from
  CUSTOMER
GROUP BY
  C_NATIONKEY