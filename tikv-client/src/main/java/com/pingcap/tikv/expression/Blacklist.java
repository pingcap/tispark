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

package com.pingcap.tikv.expression;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class Blocklist {
  private final Set<String> unsupported = new HashSet<>();

  Blocklist(String string) {
    if (string != null) {
      String[] some = string.split(",");
      for (String one : some) {
        String trimmedExprName = one.trim();
        if (!trimmedExprName.isEmpty()) {
          unsupported.add(one.trim());
        }
      }
    }
  }

  boolean isUnsupported(String name) {
    return unsupported.contains(name);
  }

  boolean isUnsupported(Class<?> cls) {
    return isUnsupported(requireNonNull(cls).getSimpleName());
  }

  @Override
  public String toString() {
    return unsupported.stream().collect(Collectors.joining(","));
  }
}
