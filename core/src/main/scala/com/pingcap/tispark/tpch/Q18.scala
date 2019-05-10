package com.pingcap.tispark.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}

class Q18 extends TpchQuery {
  override def execute(spark: SparkSession): DataFrame =
    spark.sql("""
                |select
                |	c_name,
                |	c_custkey,
                |	o_orderkey,
                |	o_orderdate,
                |	o_totalprice,
                |	sum(l_quantity)
                |from
                |	customer,
                |	orders,
                |	lineitem
                |where
                |	o_orderkey in (
                |		select
                |			l_orderkey
                |		from
                |			lineitem
                |		group by
                |			l_orderkey having
                |				sum(l_quantity) > 300
                |	)
                |	and c_custkey = o_custkey
                |	and o_orderkey = l_orderkey
                |group by
                |	c_name,
                |	c_custkey,
                |	o_orderkey,
                |	o_orderdate,
                |	o_totalprice
                |order by
                |	o_totalprice desc,
                |	o_orderdate
      """.stripMargin)
}
