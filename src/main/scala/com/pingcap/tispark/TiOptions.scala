package com.pingcap.tispark


class TiOptions(val addresses: List[String],
                val databaseName: String,
                val tableName: String) extends Serializable {
}
