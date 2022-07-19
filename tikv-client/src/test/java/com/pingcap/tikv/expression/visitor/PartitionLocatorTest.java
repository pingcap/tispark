/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pingcap.tikv.expression.visitor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.pingcap.tikv.expression.ComparisonBinaryExpression.Operator;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.Test;

public class PartitionLocatorTest {

  @Test
  public void testTimeStamp() {
    PartitionLocator locator = new PartitionLocator();
    // =
    assertTrue(
        locator.evaluateComparison(
            Timestamp.valueOf("1995-01-01 00:00:00"),
            "1995-01-01 00:00:00",
            Operator.GREATER_EQUAL));
    assertFalse(
        locator.evaluateComparison(
            Timestamp.valueOf("1995-01-01 00:00:00"), "1995-01-01 00:00:00", Operator.LESS_THAN));
    // >
    assertTrue(
        locator.evaluateComparison(
            Timestamp.valueOf("1995-01-01 10:00:00"),
            "1995-01-01 00:00:00",
            Operator.GREATER_EQUAL));
    assertFalse(
        locator.evaluateComparison(
            Timestamp.valueOf("1995-01-01 10:00:00"), "1995-01-01 00:00:00", Operator.LESS_THAN));
    // <
    assertFalse(
        locator.evaluateComparison(
            Timestamp.valueOf("1995-01-01 10:00:00"),
            "1995-01-02 00:00:00",
            Operator.GREATER_EQUAL));
    assertTrue(
        locator.evaluateComparison(
            Timestamp.valueOf("1995-01-01 10:00:00"), "1995-01-02 00:00:00", Operator.LESS_THAN));
  }

  @Test
  public void testDate() {
    PartitionLocator locator = new PartitionLocator();
    // =
    assertTrue(
        locator.evaluateComparison(
            Date.valueOf("1995-01-01"), "1995-01-01", Operator.GREATER_EQUAL));
    assertFalse(
        locator.evaluateComparison(Date.valueOf("1995-01-01"), "1995-01-01", Operator.LESS_THAN));
    // >
    assertTrue(
        locator.evaluateComparison(
            Date.valueOf("1995-02-01"), "1995-01-01", Operator.GREATER_EQUAL));
    assertFalse(
        locator.evaluateComparison(Date.valueOf("1995-02-01"), "1995-01-01", Operator.LESS_THAN));
    // <
    assertFalse(
        locator.evaluateComparison(
            Date.valueOf("1995-01-01"), "1995-02-01", Operator.GREATER_EQUAL));
    assertTrue(
        locator.evaluateComparison(Date.valueOf("1995-01-01"), "1995-02-01", Operator.LESS_THAN));
  }

  @Test
  public void testShort() {
    PartitionLocator locator = new PartitionLocator();
    // =
    assertTrue(locator.evaluateComparison((short) 56, "56", Operator.GREATER_EQUAL));
    assertFalse(locator.evaluateComparison((short) 56, "56", Operator.LESS_THAN));
    // >
    assertTrue(locator.evaluateComparison((short) 119, "56", Operator.GREATER_EQUAL));
    assertFalse(locator.evaluateComparison((short) 119, "56", Operator.LESS_THAN));
    // <
    assertFalse(locator.evaluateComparison((short) 56, "119", Operator.GREATER_EQUAL));
    assertTrue(locator.evaluateComparison((short) 56, "119", Operator.LESS_THAN));
  }

  @Test
  public void testLong() {
    PartitionLocator locator = new PartitionLocator();
    // =
    assertTrue(locator.evaluateComparison((long) 56, "56", Operator.GREATER_EQUAL));
    assertFalse(locator.evaluateComparison((long) 56, "56", Operator.LESS_THAN));
    // >
    assertTrue(locator.evaluateComparison((long) 119, "56", Operator.GREATER_EQUAL));
    assertFalse(locator.evaluateComparison((long) 119, "56", Operator.LESS_THAN));
    // <
    assertFalse(locator.evaluateComparison((long) 56, "119", Operator.GREATER_EQUAL));
    assertTrue(locator.evaluateComparison((long) 56, "119", Operator.LESS_THAN));
  }

  @Test
  public void testString() {
    PartitionLocator locator = new PartitionLocator();
    // =
    assertTrue(locator.evaluateComparison("long", "long", Operator.GREATER_EQUAL));
    assertFalse(locator.evaluateComparison("long", "long", Operator.LESS_THAN));
    // >
    assertTrue(locator.evaluateComparison("long", "loNg", Operator.GREATER_EQUAL));
    assertFalse(locator.evaluateComparison("long", "long", Operator.LESS_THAN));
    // <
    assertFalse(locator.evaluateComparison("LoNg", "loNg", Operator.GREATER_EQUAL));
    assertTrue(locator.evaluateComparison("LoNg", "loNg", Operator.LESS_THAN));
  }
}
