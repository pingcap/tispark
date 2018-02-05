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

package com.pingcap.tikv.meta;

import java.io.Serializable;

public class TiAnalyzeRequest implements Serializable {
  private static final int maxSampleSize        = 10000;
  private static final int maxRegionSampleSize  = 1000;
  private static final int maxSketchSize        = 10000;
  private static final int maxBucketSize        = 256;
  private static final int defaultCMSketchDepth = 5;
  private static final int defaultCMSketchWidth = 2048;
}
