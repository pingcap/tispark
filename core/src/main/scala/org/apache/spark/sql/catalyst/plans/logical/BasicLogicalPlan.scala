package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.ReflectionUtil

object BasicLogicalPlan {
  def extractAuthorizationRule(
      logicalPlan: LogicalPlan,
      tiAuthorization: Option[TiAuthorization]): LogicalPlan =
    ReflectionUtil.callTiBasicLogicalPlanExtractAuthorizationRule(logicalPlan, tiAuthorization)
}
