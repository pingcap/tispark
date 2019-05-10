package com.pingcap.tispark.tpch
import org.apache.spark.sql.{DataFrame, SparkSession}
class Q22 extends TpchQuery {
  override def execute(spark: SparkSession): DataFrame =
    spark.sql("""
                |      select
                |	cntrycode,
                |	count(*) as numcust,
                |	sum(c_acctbal) as totacctbal
                | from
                |	(
                |		select
                |			substring(c_phone, 1 , 2) as cntrycode,
                |			c_acctbal
                |		from
                |			customer
                |		where
                |			substring(c_phone , 1 , 2) in
                |				('20', '40', '22', '30', '39', '42', '21')
                |			and c_acctbal > (
                |				select
                |					avg(c_acctbal)
                |				from
                |					customer
                |				where
                |					c_acctbal > 0.00
                |					and substring(c_phone , 1 , 2) in
                |						('20', '40', '22', '30', '39', '42', '21')
                |			)
                |			and not exists (
                |				select
                |					*
                |				from
                |					orders
                |				where
                |					o_custkey = c_custkey
                |			)
                |	) as custsale
                | group by
                |	cntrycode
                | order by
                |	cntrycode
      	""".stripMargin)
}
