# TiSpark Type System

TODO
- fix ci
- MultiColumnPKDataTypeSuites:
  - select col_bit0 from test_2110772277 where col_boolean0 > 1
  - select col_boolean0 from test_2110772277 where col_bit1 > 1
- add order by to compare
- year pushdown bug
- document
- support v1 in tikv test
- support v1 in core test
- delete special compare logic

| TiDB Type            | JDBC                 | Spark JDBC         | TiSpark V0         | TiSpark V1         | diff |
| -------------------- | -------------------- | ------------------ | ------------------ | ------------------ | ---- |
| BIT(1)               | java.lang.Boolean    | BooleanType        | LongType           | BooleanType        | *    |
| BIT( > 1)            | byte[]               | BooleanType        | LongType           | BinaryType         | *    |
| TINYINT              | java.lang.Integer    | IntegerType        | LongType           | IntegerType        | *    |
| BOOLEAN              | java.lang.Boolean    | BooleanType        | LongType           | BooleanType        | *    |
| SMALLINT [UNSIGNED]  | java.lang.Integer    | IntegerType        | LongType           | IntegerType        | *    |
| MEDIUMINT [UNSIGNED] | java.lang.Integer    | IntegerType        | LongType           | IntegerType        | *    |
| INT                  | java.lang.Integer    | IntegerType        | LongType           | IntegerType        | *    |
| INT UNSIGNED         | java.lang.Long       | LongType           | LongType           | LongType           |      |
| BIGINT               | java.lang.Long       | LongType           | LongType           | LongType           |      |
| BIGINT UNSIGNED      | java.math.BigInteger | DecimalType(20, 0) | DecimalType(20, 0) | DecimalType(20, 0) |      |
| FLOAT                | java.lang.Float      | DoubleType         | DoubleType         | DoubleType         |      |
| DOUBLE               | java.lang.Double     | DoubleType         | DoubleType         | DoubleType         |      |
| DECIMAL              | java.math.BigDecimal | DecimalType        | DecimalType        | DecimalType        |      |
| DATE                 | java.sql.Date        | DateType           | DateType           | DateType           |      |
| DATETIME             | java.sql.Timestamp   | TimestampType      | TimestampType      | TimestampType      |      |
| TIMESTAMP            | java.sql.Timestamp   | TimestampType      | TimestampType      | TimestampType      |      |
| TIME                 | java.sql.Time        | TimestampType      | LongType           | TimestampType      | *    |
| YEAR                 | java.sql.Date        | DateType           | LongType           | DateType           | *    |
| CHAR                 | java.lang.String     | StringType         | StringType         | StringType         |      |
| VARCHAR              | java.lang.String     | StringType         | StringType         | StringType         |      |
| BINARY               | byte[]               | BinaryType         | BinaryType         | BinaryType         |      |
| VARBINARY            | byte[]               | BinaryType         | BinaryType         | BinaryType         |      |
| TINYBLOB             | byte[]               | BinaryType         | BinaryType         | BinaryType         |      |
| TINYTEXT             | java.lang.String     | StringType         | StringType         | StringType         |      |
| BLOB                 | byte[]               | BinaryType         | BinaryType         | BinaryType         |      |
| TEXT                 | java.lang.String     | StringType         | StringType         | StringType         |      |
| MEDIUMBLOB           | byte[]               | BinaryType         | BinaryType         | BinaryType         |      |
| MEDIUMTEXT           | java.lang.String     | StringType         | StringType         | StringType         |      |
| LONGBLOB             | byte[]               | BinaryType         | BinaryType         | BinaryType         |      |
| LONGTEXT             | java.lang.String     | StringType         | StringType         | StringType         |      |
| ENUM                 | java.lang.String     | StringType         | StringType         | StringType         |      |
| SET                  | java.lang.String     | StringType         | StringType         | StringType         |      |
