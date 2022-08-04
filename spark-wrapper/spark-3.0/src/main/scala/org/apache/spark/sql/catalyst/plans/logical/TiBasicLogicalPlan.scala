package org.apache.spark.sql.catalyst.plans.logical

import com.pingcap.tispark.auth.TiAuthorization

class TiBasicLogicalPlan {
  def extractAuthorizationRule(logicalPlan: LogicalPlan, tiAuthorization: Option[TiAuthorization]): LogicalPlan = {
    logicalPlan match {
      case st@SetCatalogAndNamespace(catalogManager,catalogName,namespace) =>
        if (namespace.isDefined) {
          namespace.get
            .foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
        }
        st
    }
  }

}
