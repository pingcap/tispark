from py4j.java_gateway import java_import
from pyspark.context import SparkContext


class TiContext:
    # create a new TiContext
    def __init__(self, sparkSession):
        SparkContext._ensure_initialized()
        gw = SparkContext._gateway
        java_import(gw.jvm, "org.apache.spark.sql.TiContext")
        self.ti = gw.jvm.TiContext(sparkSession._jsparkSession)

    def getContext(self):
        return self.ti

    def tidbMapDatabase(self, dbName, isPrefix):
        self.ti.tidbMapDatabase(dbName, isPrefix)