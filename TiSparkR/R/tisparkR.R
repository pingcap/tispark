# Title     : TiSparkR
# Objective : TiSpark entry for R
# Created by: novemser
# Created on: 17-11-1

createTiContext <- function(session) {
  sparkR.newJObject("org.apache.spark.sql.TiContext", session)
}

tidbMapDatabase <- function(tiContext, dbName) {
  sparkR.callJMethod(tiContext, "tidbMapDatabase", dbName, FALSE)
  paste("Mapping to database:", dbName)
}
