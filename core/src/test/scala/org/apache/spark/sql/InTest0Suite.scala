/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class InTest0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select tp_int from full_data_type_table  where tp_int in (2333, 601508558, 4294967296, 4294967295)") {
    val r1 = querySpark("select tp_int from full_data_type_table  where tp_int in (2333, 601508558, 4294967296, 4294967295)")
    val r2 = querySpark("select tp_int from full_data_type_table_j  where tp_int in (2333, 601508558, 4294967296, 4294967295)")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint from full_data_type_table  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)") {
    val r1 = querySpark("select tp_bigint from full_data_type_table  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)")
    val r2 = querySpark("select tp_bigint from full_data_type_table_j  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_varchar from full_data_type_table  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')") {
    val r1 = querySpark("select tp_varchar from full_data_type_table  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')")
    val r2 = querySpark("select tp_varchar from full_data_type_table_j  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_decimal from full_data_type_table  where tp_decimal in (2, 3, 4)") {
    val r1 = querySpark("select tp_decimal from full_data_type_table  where tp_decimal in (2, 3, 4)")
    val r2 = querySpark("select tp_decimal from full_data_type_table_j  where tp_decimal in (2, 3, 4)")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_double from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)") {
    val r1 = querySpark("select tp_double from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)")
    val r2 = querySpark("select tp_double from full_data_type_table_j  where tp_double in (0.2054466,3.1415926,0.9412022)")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_float from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)") {
    val r1 = querySpark("select tp_float from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)")
    val r2 = querySpark("select tp_float from full_data_type_table_j  where tp_double in (0.2054466,3.1415926,0.9412022)")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_datetime from full_data_type_table  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')") {
    val r1 = querySpark("select tp_datetime from full_data_type_table  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')")
    val r2 = querySpark("select tp_datetime from full_data_type_table_j  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_date from full_data_type_table  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')") {
    val r1 = querySpark("select tp_date from full_data_type_table  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')")
    val r2 = querySpark("select tp_date from full_data_type_table_j  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_timestamp from full_data_type_table  where tp_timestamp in ('2017-11-02 16:48:01')") {
    val r1 = querySpark("select tp_timestamp from full_data_type_table  where tp_timestamp in ('2017-11-02 16:48:01')")
    val r2 = querySpark("select tp_timestamp from full_data_type_table_j  where tp_timestamp in ('2017-11-02 16:48:01')")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_real from full_data_type_table  where tp_real in (4.44,0.5194052764001038)") {
    val r1 = querySpark("select tp_real from full_data_type_table  where tp_real in (4.44,0.5194052764001038)")
    val r2 = querySpark("select tp_real from full_data_type_table_j  where tp_real in (4.44,0.5194052764001038)")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_longtext from full_data_type_table  where tp_longtext in ('很长的一段文字', 'OntPHB22qwSxriGUQ9RLfoiRkEMfEYFZdnAkL7SdpfD59MfmUXpKUAXiJpegn6dcMyfRyBhNw9efQfrl2yMmtM0zJx3ScAgTIA8djNnmCnMVzHgPWVYfHRnl8zENOD5SbrI4HAazss9xBVpikAgxdXKvlxmhfNoYIK0YYnO84MXKkMUinjPQ7zWHbh5lImp7g9HpIXgtkFFTXVvCaTr8mQXXOl957dxePeUvPv28GUdnzXTzk7thTbsWAtqU7YaK4QC4z9qHpbt5ex9ck8uHz2RoptFw71RIoKGiPsBD9YwXAS19goDM2H0yzVtDNJ6ls6jzXrGlJ6gIRG73Er0tVyourPdM42a5oDihfVP6XxjOjS0cmVIIppDSZIofkRfRhQWAunheFbEEPSHx3eybQ6pSIFd34Natgr2erFjyxFIRr7J535HT9aIReYIlocKK2ZI9sfcwhX0PeDNohY2tvHbsrHE0MlKCyVSTjPxszvFjCPlyqwQy')") {
    val r1 = querySpark("select tp_longtext from full_data_type_table  where tp_longtext in ('很长的一段文字', 'OntPHB22qwSxriGUQ9RLfoiRkEMfEYFZdnAkL7SdpfD59MfmUXpKUAXiJpegn6dcMyfRyBhNw9efQfrl2yMmtM0zJx3ScAgTIA8djNnmCnMVzHgPWVYfHRnl8zENOD5SbrI4HAazss9xBVpikAgxdXKvlxmhfNoYIK0YYnO84MXKkMUinjPQ7zWHbh5lImp7g9HpIXgtkFFTXVvCaTr8mQXXOl957dxePeUvPv28GUdnzXTzk7thTbsWAtqU7YaK4QC4z9qHpbt5ex9ck8uHz2RoptFw71RIoKGiPsBD9YwXAS19goDM2H0yzVtDNJ6ls6jzXrGlJ6gIRG73Er0tVyourPdM42a5oDihfVP6XxjOjS0cmVIIppDSZIofkRfRhQWAunheFbEEPSHx3eybQ6pSIFd34Natgr2erFjyxFIRr7J535HT9aIReYIlocKK2ZI9sfcwhX0PeDNohY2tvHbsrHE0MlKCyVSTjPxszvFjCPlyqwQy')")
    val r2 = querySpark("select tp_longtext from full_data_type_table_j  where tp_longtext in ('很长的一段文字', 'OntPHB22qwSxriGUQ9RLfoiRkEMfEYFZdnAkL7SdpfD59MfmUXpKUAXiJpegn6dcMyfRyBhNw9efQfrl2yMmtM0zJx3ScAgTIA8djNnmCnMVzHgPWVYfHRnl8zENOD5SbrI4HAazss9xBVpikAgxdXKvlxmhfNoYIK0YYnO84MXKkMUinjPQ7zWHbh5lImp7g9HpIXgtkFFTXVvCaTr8mQXXOl957dxePeUvPv28GUdnzXTzk7thTbsWAtqU7YaK4QC4z9qHpbt5ex9ck8uHz2RoptFw71RIoKGiPsBD9YwXAS19goDM2H0yzVtDNJ6ls6jzXrGlJ6gIRG73Er0tVyourPdM42a5oDihfVP6XxjOjS0cmVIIppDSZIofkRfRhQWAunheFbEEPSHx3eybQ6pSIFd34Natgr2erFjyxFIRr7J535HT9aIReYIlocKK2ZI9sfcwhX0PeDNohY2tvHbsrHE0MlKCyVSTjPxszvFjCPlyqwQy')")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_text from full_data_type_table  where tp_text in ('一般的文字', 'dQWD3XwSTevpbP5hADFdNO0dQvaueFhnGcJAm045mGv5fXttso')") {
    val r1 = querySpark("select tp_text from full_data_type_table  where tp_text in ('一般的文字', 'dQWD3XwSTevpbP5hADFdNO0dQvaueFhnGcJAm045mGv5fXttso')")
    val r2 = querySpark("select tp_text from full_data_type_table_j  where tp_text in ('一般的文字', 'dQWD3XwSTevpbP5hADFdNO0dQvaueFhnGcJAm045mGv5fXttso')")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}