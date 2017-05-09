package com.pingcap.tispark

import com.pingcap.tidb.tipb.SelectRequest
import com.pingcap.tispark.TiCoprocessorOperation.CoprocessorReq


object CoprocessorUtils {
  def generateProtoReq(cop: CoprocessorReq): SelectRequest = {
    // TODO: Add select request builder
    SelectRequest.newBuilder build
  }
}
