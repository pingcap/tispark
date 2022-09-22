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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.util;

import java.lang.reflect.Method;

public class ReflectionWrapper {
  private final Object obj;
  private final Class<?>[] classes;
  private static final IllegalArgumentException WRONG_NUMBER_OF_ARGS_EXCEPTION =
      new IllegalArgumentException("wrong number of arguments");

  public ReflectionWrapper(Object obj, Class<?>... classes) {
    this.obj = obj;
    this.classes = classes;
  }

  public Object call(String methodName, Object... args) {
    // mock call duration
    try {
      Thread.sleep(1);
    } catch (Exception ignored) {
      // ignore
    }
    if (classes.length != args.length) {
      throw WRONG_NUMBER_OF_ARGS_EXCEPTION;
    }
    try {
      Method method = obj.getClass().getDeclaredMethod(methodName, classes);
      method.setAccessible(true);
      return method.invoke(obj, args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
