/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.auth

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

object MySQLPriv extends Enumeration {

  // UsagePriv is a synonym for “no privileges”
  val UsagePriv,
      // CreatePriv is the privilege to create schema/table.
  CreatePriv,
      // SelectPriv is the privilege to read from table.
  SelectPriv,
      // InsertPriv is the privilege to insert data into table.
  InsertPriv,
      // UpdatePriv is the privilege to update data in table.
  UpdatePriv,
      // DeletePriv is the privilege to delete data from table.
  DeletePriv,
      // ShowDBPriv is the privilege to run show databases statement.
  ShowDBPriv,
      // SuperPriv enables many operations and server behaviors.
  SuperPriv,
      // CreateUserPriv is the privilege to create user.
  CreateUserPriv,
      // TriggerPriv is not checked yet.
  TriggerPriv,
      // DropPriv is the privilege to drop schema/table.
  DropPriv,
      // ProcessPriv pertains to display of information about the threads executing within the server.
  ProcessPriv,
      // GrantPriv is the privilege to grant privilege to user.
  GrantPriv,
      // ReferencesPriv is not checked yet.
  ReferencesPriv,
      // AlterPriv is the privilege to run alter statement.
  AlterPriv,
      // ExecutePriv is the privilege to run execute statement.
  ExecutePriv,
      // IndexPriv is the privilege to create/drop index.
  IndexPriv,
      // CreateViewPriv is the privilege to create view.
  CreateViewPriv,
      // ShowViewPriv is the privilege to show create view.
  ShowViewPriv,
      // CreateRolePriv the privilege to create a role.
  CreateRolePriv,
      // DropRolePriv is the privilege to drop a role.
  DropRolePriv,
      // CreateTMPTablePriv is the privilege to create a temporary table.
  CreateTMPTablePriv, LockTablesPriv, CreateRoutinePriv, AlterRoutinePriv, EventPriv,
      // ShutdownPriv the privilege to shutdown a server.
  ShutdownPriv,
      // ReloadPriv is the privilege to enable the use of the FLUSH statement.
  ReloadPriv,
      // FilePriv is the privilege to enable the use of LOAD DATA and SELECT ... INTO OUTFILE.
  FilePriv,
      // ConfigPriv is the privilege to enable the use SET CONFIG statements.
  ConfigPriv,
      // CreateTablespacePriv is the privilege to create tablespace.
  CreateTablespacePriv,
      // ReplicationClientPriv is used in MySQL replication
  ReplicationClientPriv,
      // ReplicationSlavePriv is used in MySQL replication
  ReplicationSlavePriv,
      // AllPriv is the privilege for all actions.
  AllPriv, /*
   *  Please add the new priv before AllPriv to keep the values consistent across versions.
   */

  // ExtendedPriv is used to successful parse privileges not included above.
  // these are dynamic privileges in MySQL 8.0 and other extended privileges like LOAD FROM S3 in Aurora.
  ExtendedPriv = Value

  final val Str2Priv = CaseInsensitiveMap(
    Map(
      "CREATE" -> MySQLPriv.CreatePriv,
      "SELECT" -> MySQLPriv.SelectPriv,
      "INSERT" -> MySQLPriv.InsertPriv,
      "UPDATE" -> MySQLPriv.UpdatePriv,
      "DELETE" -> MySQLPriv.DeletePriv,
      "SHOW DATABASES" -> MySQLPriv.ShowDBPriv,
      "SUPER" -> MySQLPriv.SuperPriv,
      "CREATE USER" -> MySQLPriv.CreateUserPriv,
      "CREATE TABLESPACE" -> MySQLPriv.CreateTablespacePriv,
      "TRIGGER" -> MySQLPriv.TriggerPriv,
      "DROP" -> MySQLPriv.DropPriv,
      "PROCESS" -> MySQLPriv.ProcessPriv,
      "GRANT OPTION" -> MySQLPriv.GrantPriv,
      "REFERENCES" -> MySQLPriv.ReferencesPriv,
      "ALTER" -> MySQLPriv.AlterPriv,
      "EXECUTE" -> MySQLPriv.ExecutePriv,
      "INDEX" -> MySQLPriv.IndexPriv,
      "CREATE VIEW" -> MySQLPriv.CreateViewPriv,
      "SHOW VIEW" -> MySQLPriv.ShowViewPriv,
      "CREATE ROLE" -> MySQLPriv.CreateRolePriv,
      "DROP ROLE" -> MySQLPriv.DropRolePriv,
      "CREATE TEMPORARY TABLES" -> MySQLPriv.CreateTMPTablePriv,
      "LOCK TABLES" -> MySQLPriv.LockTablesPriv,
      "CREATE ROUTINE" -> MySQLPriv.CreateRoutinePriv,
      "ALTER ROUTINE" -> MySQLPriv.AlterRoutinePriv,
      "EVENT" -> MySQLPriv.EventPriv,
      "SHUTDOWN" -> MySQLPriv.ShutdownPriv,
      "RELOAD" -> MySQLPriv.ReloadPriv,
      "FILE" -> MySQLPriv.FilePriv,
      "CONFIG" -> MySQLPriv.ConfigPriv,
      "USAGE" -> MySQLPriv.UsagePriv,
      "REPLICATION CLIENT" -> MySQLPriv.ReplicationClientPriv,
      "REPLICATION SLAVE" -> MySQLPriv.ReplicationSlavePriv,
      "ALL PRIVILEGES" -> MySQLPriv.AllPriv))
}
