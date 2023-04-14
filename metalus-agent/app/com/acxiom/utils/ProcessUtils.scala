package com.acxiom.utils

import java.lang.management.ManagementFactory

object ProcessUtils {
  /**
   * Returns limited information about this host.
   *
   * @return Host information
   */
  def hostInfo: HostInfo = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
    HostInfo(osBean.getAvailableProcessors, osBean.getSystemLoadAverage, osBean.getArch)
  }
}

case class HostInfo(cpus: Int, avgLoad: Double, arch: String)
