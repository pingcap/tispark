package com.pingcap.tikv.util;

import com.pingcap.tikv.columnar.datatypes.CHType;
import com.pingcap.tikv.columnar.datatypes.CHTypeDate;
import com.pingcap.tikv.columnar.datatypes.CHTypeDateTime;
import com.pingcap.tikv.columnar.datatypes.CHTypeDecimal;
import com.pingcap.tikv.columnar.datatypes.CHTypeFixedString;
import com.pingcap.tikv.columnar.datatypes.CHTypeMyDate;
import com.pingcap.tikv.columnar.datatypes.CHTypeMyDateTime;
import com.pingcap.tikv.columnar.datatypes.CHTypeNumber;
import com.pingcap.tikv.columnar.datatypes.CHTypeString;
import org.apache.commons.lang3.StringUtils;

public class CHTypeMapping {
  public static CHType parseType(String typeName) {
    if (typeName == null || typeName.isEmpty()) {
      throw new UnsupportedOperationException("Empty CH type!");
    }
    typeName = typeName.trim();
    switch (typeName) {
      case "UInt8":
        return CHTypeNumber.CHTypeUInt8.instance;
      case "UInt16":
        return CHTypeNumber.CHTypeUInt16.instance;
      case "UInt32":
        return CHTypeNumber.CHTypeUInt32.instance;
      case "UInt64":
        return CHTypeNumber.CHTypeUInt64.instance;
      case "Int8":
        return CHTypeNumber.CHTypeInt8.instance;
      case "Int16":
        return CHTypeNumber.CHTypeInt16.instance;
      case "Int32":
        return CHTypeNumber.CHTypeInt32.instance;
      case "Int64":
        return CHTypeNumber.CHTypeInt64.instance;
      case "Float32":
        return CHTypeNumber.CHTypeFloat32.instance;
      case "Float64":
        return CHTypeNumber.CHTypeFloat64.instance;
      case "Date":
        return CHTypeDate.instance;
      case "DateTime":
        return CHTypeDateTime.instance;
      case "MyDateTime":
        return CHTypeMyDateTime.instance;
      case "MyDate":
        return CHTypeMyDate.instance;
      case "String":
        return CHTypeString.instance;
    }
    if (typeName.startsWith("FixedString")) {
      String remain = StringUtils.removeStart(typeName, "FixedString");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      try {
        int length = Integer.parseInt(remain);
        return new CHTypeFixedString(length);
      } catch (NumberFormatException e) {
        throw new UnsupportedOperationException("Illegal CH type: " + typeName);
      }
    }
    if (typeName.startsWith("MyDateTime")) {
      return CHTypeMyDateTime.instance;
    }
    if (typeName.startsWith("Decimal")) {
      String remain = StringUtils.removeStart(typeName, "Decimal");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      try {
        String[] args = remain.split(",");
        int precision = Integer.parseInt(args[0]);
        int scale = Integer.parseInt(args[1]);
        return new CHTypeDecimal(precision, scale);
      } catch (Exception e) {
        throw new UnsupportedOperationException("Illegal CH type: " + typeName);
      }
    }
    if (typeName.startsWith("Nullable")) {
      String remain = StringUtils.removeStart(typeName, "Nullable");
      remain = StringUtils.removeEnd(StringUtils.removeStart(remain, "("), ")");
      CHType type = parseType(remain);
      type.setNullable(true);
      return type;
    }
    throw new UnsupportedOperationException("Unsupported CH type: " + typeName);
  }
}
