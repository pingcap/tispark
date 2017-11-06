package com.pingcap.spark;

import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Random;
import java.util.UUID;

public class DAGDataGenerator extends JDBCDataGenerator {
  private Random random = new Random();

  private void insertRandomData() throws SQLException {
    PreparedStatement pStmt = prepareInsert();
    int BEGIN_NUM = 1002;
    int INSERT_NUM = 1000;
    long start = Timestamp.valueOf("1970-01-02 00:00:00").getTime();
    long end = Timestamp.valueOf("2113-01-01 00:00:00").getTime();
    long tsEnd = Timestamp.valueOf("2038-01-19 03:14:07").getTime();
    long diff = end - start + 1;
    long tsDiff = tsEnd - start + 1;

    for (int i = BEGIN_NUM; i <= BEGIN_NUM + INSERT_NUM; i++) {
      pStmt.setInt(1, i);
      pStmt.setString(2, UUID.randomUUID().toString());
      pStmt.setBlob(3, (Blob) null);
      pStmt.setBlob(4, (Blob) null);
      pStmt.setDate(5, new Date(randomTimestamp(start, diff)));
      pStmt.setInt(6, 2017);
      pStmt.setLong(7, random.nextLong());
      pStmt.setBigDecimal(8, new BigDecimal(i + "." + i * 2));
      pStmt.setFloat(9, random.nextFloat());
      pStmt.setDouble(10, random.nextDouble());
      pStmt.setInt(11, random.nextInt());
      pStmt.setInt(12, random.nextInt(8388607));
      pStmt.setDouble(13, random.nextDouble());
      pStmt.setInt(14, random.nextInt(32767));
      pStmt.setInt(15, random.nextInt(127));
      pStmt.setString(16, randomString(10));
      pStmt.setString(17, randomString(5));
      pStmt.setString(18, randomString(500));
      pStmt.setString(19, randomString(100));
      pStmt.setString(20, randomString(50));
      pStmt.setString(21, randomString(20));
      pStmt.setBoolean(22, (i & 1) == 0);
      pStmt.setTime(23, new Time(randomTimestamp(start, diff)));
      pStmt.setTimestamp(24, new Timestamp(randomTimestamp(start, tsDiff)));
      pStmt.setDate(25, new Date(randomTimestamp(start, diff)));
      pStmt.execute();
      System.out.println("Insert " + i + " succeed.");
    }
  }

  private PreparedStatement prepareInsert() throws SQLException {
    return connection.prepareStatement(
        "INSERT INTO tispark_test.full_data_type_table(" +
            "id_dt, tp_varchar, " +       //1,2
            "tp_blob, tp_binary, " +      //3,4
            "tp_date, tp_year, " +        //5,6
            "tp_bigint, tp_decimal, " +   //7,8
            "tp_double, tp_float, " +     //9,10
            "tp_int, tp_mediumint, " +    //11,12
            "tp_real, tp_smallint, " +    //13,14
            "tp_tinyint, tp_char, " +     //15,16
            "tp_nvarchar, tp_longtext, " +//17,18
            "tp_mediumtext, tp_text, " +  //19,20
            "tp_tinytext, tp_bit, " +     //21,22
            "tp_time, tp_timestamp, " +   //23,24
            "tp_datetime " +              //25
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    );
  }

  private String randomString(int length) {
    RandomStringGenerator generator =
        new RandomStringGenerator.Builder()
            .withinRange('0', 'z')
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS)
            .build();
    return generator.generate(length);
  }

  private Long randomTimestamp(long start, long diff) {
    return start + (long) (diff * Math.random());
  }

  public static void main(String[] args) throws SQLException {
    DAGDataGenerator generator = new DAGDataGenerator();
    generator.initConnection();
    generator.insertRandomData();
  }
}
