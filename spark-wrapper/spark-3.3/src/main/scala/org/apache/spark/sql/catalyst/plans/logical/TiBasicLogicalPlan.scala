package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tispark.auth.TiAuthorization
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace

class TiBasicLogicalPlan {
  def verifyAuthorizationRule(
      logicalPlan: LogicalPlan,
      tiAuthorization: Option[TiAuthorization]): LogicalPlan = {
    logicalPlan match {
      case st @ SetCatalogAndNamespace(ResolvedNamespace(catalog, ns)) =>
        ns.foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
        st
    }
  }

}
