/*
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
 */

package com.pingcap.tikv.types;

import com.pingcap.tikv.exception.TypeException;

import java.util.List;

/**
 * Sample logic for MySQL Enum
 *
 * TODO: make it work after we support enum type
 */
public class Enum {
  private final String name;
  private final long value;

  public Enum(String name, long value) {
    this.name = name;
    this.value = value;
  }

  public static Enum create(String name, long value) {
    return new Enum(name, value);
  }

  /**
   * Parsing an Enum by its name.
   * for enum {'y', 'n'}, we can use 'y' or its 1-base index '1' to represent enum('y')
   *
   * @param elems enum name list
   * @param name parameter to find enum
   * @return if enum found: an Enum{name: String, value: long} with its name and index
   *         if not found: throws TypeException
   */
  public static Enum ParseEnumName(List<String> elems, String name) {
    int i = 1;
    for (String elem: elems) {
      if (elem.equalsIgnoreCase(name)) {
        return create(elem, i);
      }
      i ++;
    }
    try {
      // name doesn't exist, maybe an integer?
      int num = Integer.valueOf(name);
      return ParseEnumValue(elems, num);
    } catch (TypeException e) {
      throw e;
    } catch (Exception e) {
      throw new TypeException("item " + name + " is not in enum " + elems);
    }
  }

  private static Enum ParseEnumValue(List<String> elems, int number) {
    if (number <= 0 || number > elems.size()) {
      throw new TypeException("number " + number + " overflow enum boundary [1, " + elems.size() + "]");
    }
    return create(elems.get(number - 1), number);
  }

  public double toNumber() {
    return ((double) value);
  }

  public long getValue() {
    return value;
  }

  public String getName() {
    return name;
  }
}
