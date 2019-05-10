package com.pingcap.tispark.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

class Q04 extends TpchQuery {
  override def execute(spark: SparkSession): DataFrame =
    spark.sql("""select
                |	o_orderpriority,
                |	count(*) as order_count
                |from
                |	orders
                |where
                |	o_orderdate >= date '1993-07-01'
                |	and o_orderdate < date '1993-07-01' + interval '3' month
                |	and exists (
                |		select
                |			*
                |		from
                |			lineitem
                |		where
                |			l_orderkey = o_orderkey
                |			and l_commitdate < l_receiptdate
                |	)
                |group by
                |	o_orderpriority
                |order by
                |	o_orderpriority
      """.stripMargin)
}
