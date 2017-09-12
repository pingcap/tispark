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

package com.pingcap.tispark


object TiConfigConst {
  val PD_ADDRESSES: String = "spark.tispark.pd.addresses"
  val GRPC_FRAME_SIZE: String = "spark.tispark.grpc.framesize"
  val GRPC_TIMEOUT: String = "spark.tispark.grpc.timeout_in_sec"
  val META_RELOAD_PERIOD: String = "spark.tispark.meta.reload_period_in_sec"
  val GRPC_RETRY_TIMES: String = "spark.tispark.grpc.retry.times"
}
