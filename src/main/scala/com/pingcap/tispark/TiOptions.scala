package com.pingcap.tispark


class TiOptions extends Serializable {
  def addresses = List("127.0.0.1:" + 2379)
  def tableName = "t2"
  def databaseName = "test"
}
