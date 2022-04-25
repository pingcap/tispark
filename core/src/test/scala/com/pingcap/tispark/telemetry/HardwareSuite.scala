package com.pingcap.tispark.telemetry

import org.scalatest.FunSuite
import oshi.SystemInfo

class HardwareSuite extends FunSuite{

  test ("xxx" ) {
    val si = new SystemInfo()
    val hal = si.getHardware
    val ope = si.getOperatingSystem
    val cpu = hal.getProcessor
    val sys = hal.getComputerSystem
    val proce = hal.getProcessor
    val mem = hal.getMemory
    val disk = hal.getDiskStores
    println("ss")
  }

  test ("xdsfs") {
    //val hardwareInfo = HardwareInfo
    //hardwareInfo.getHardwareInfo
    //println("xx")
  }


}
