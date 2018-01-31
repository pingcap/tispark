package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseTiSparkSuite

class OtherTestSuite extends BaseTiSparkSuite {
  private val cases = Seq[String](
    "select id_dt from full_data_type_table where tp_date is null",
    "select id_dt from full_data_type_table where tp_date is not null"
  )

  cases foreach { query => {
    test(query) {
      runTest(query, query.replace("full_data_type_table", "full_data_type_table_j"))
    }
  }}
}
