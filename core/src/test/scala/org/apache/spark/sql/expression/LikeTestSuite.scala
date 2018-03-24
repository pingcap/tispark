package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseTiSparkSuite

class LikeTestSuite extends BaseTiSparkSuite {
  private val tableScanCases = Seq[String](
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%a%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%a'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%a%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%a'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE '%'",
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'z%'"
  )

  private val indexScanCases = Seq[String](
    "select tp_varchar from full_data_type_table_idx where tp_varchar LIKE 'a%'",
    "select tp_varchar from full_data_type_table_idx where tp_varchar LIKE 'a%f'"
  )

  tableScanCases foreach { query =>
    {
      test(query) {
        runTest(query, query.replace("full_data_type_table", "full_data_type_table_j"))
      }
    }
  }

  indexScanCases foreach { query =>
    {
      test(query) {
        runTest(query, query.replace("full_data_type_table_idx", "full_data_type_table_idx_j"))
      }
    }
  }
}
