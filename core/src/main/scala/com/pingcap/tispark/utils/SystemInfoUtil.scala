/*
 * Copyright 2022 PingCAP, Inc.
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

package com.pingcap.tispark.utils

import oshi.SystemInfo
import oshi.hardware.CentralProcessor.ProcessorIdentifier
import oshi.hardware.{CentralProcessor, HardwareAbstractionLayer}
import oshi.software.os.{FileSystem, OperatingSystem}

import scala.collection.mutable.ListBuffer

/**
 * SystemInfoUtil is used to get system and hardware information.
 */
object SystemInfoUtil {

  private val si = new SystemInfo
  private val hal: HardwareAbstractionLayer = si.getHardware
  private val operatingSystem: OperatingSystem = si.getOperatingSystem
  private val processor: CentralProcessor = hal.getProcessor
  private val processorIdentifier: ProcessorIdentifier = processor.getProcessorIdentifier
  private val fs: FileSystem = operatingSystem.getFileSystem

  /**
   * Get the Operating System family.
   *
   * @return the system family
   */
  def getOsFamily: String = operatingSystem.getFamily

  /**
   * Get Operating System version information.
   *
   * @return version information
   */
  def getOsVersion: String = operatingSystem.getVersionInfo.toString

  /**
   * Name, eg. Intel(R) Core(TM)2 Duo CPU T7300 @ 2.00GHz
   *
   * @return Processor name.
   */
  def getCpuName: String = processorIdentifier.getName

  /**
   * Get the number of logical CPUs available for processing.
   *
   * @return The number of logical CPUs available.
   */
  def getCpuLogicalCores: String = processor.getLogicalProcessorCount.toString

  /**
   * Get the number of physical CPUs/cores available for processing.
   *
   * @return The number of physical CPUs available.
   */
  def getCpuPhysicalCore: String = processor.getPhysicalProcessorCount.toString

  def getCpu: Map[String, String] = {
    Map(
      "model" -> SystemInfoUtil.getCpuName,
      "logicalCores" -> SystemInfoUtil.getCpuLogicalCores,
      "physicalCores" -> SystemInfoUtil.getCpuPhysicalCore
    )
  }

  /**
   * Get memory size.
   *
   * @return The free and used size.
   */
  def getMemoryInfo: String = hal.getMemory.toString

  /**
   * Get disks' size and type
   *
   * @return A List of {disk name, disk size}
   */
  def getDisks: ListBuffer[Map[String, String]] = {
    val disks = new ListBuffer[Map[String, String]]()
    val diskStore = hal.getDiskStores
    for (i <- 0 until diskStore.size()) {
      disks.append(
        Map(
          "name" -> diskStore.get(i).getName,
          "size" -> diskStore.get(i).getSize.toString
        )
      )
    }
    disks
  }

  /**
   * Get filesystem
   *
   * @return
   */
  def getFileSystem: ListBuffer[Map[String, String]] = {
    val fileSystem = new ListBuffer[Map[String, String]]()
    val fileStores = fs.getFileStores
    for (i <- 0 until fileStores.size()) {
      val fileStore = fileStores.get(i)
      fileSystem.append(
        Map(
          "name" -> fileStore.getName,
          "type" -> fileStore.getDescription
        )
      )
    }
    fileSystem
  }
}
