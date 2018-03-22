package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseTiSparkSuite

/**
 * Created by birdstorm on 2018/3/21.
 */
class LikeTestSuite extends BaseTiSparkSuite {
  private val allCases = Seq[String](
    "select tp_varchar from full_data_type_table where tp_varchar LIKE 'a%'"
  )

  allCases foreach { query =>
    {
      test(query) {
        runTest(query, query.replace("full_data_type_table", "full_data_type_table_j"))
      }
    }
  }
}
