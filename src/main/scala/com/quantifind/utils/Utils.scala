package com.quantifind.utils

import java.net.InetAddress

/**
 * Basic utils
 *
 * @author xorlev
 */

object Utils {

  /**
    * Instead of giving the hostname of the consumer, Kafka broker sometimes gives "/ip4-address" value,
    * try to replace by the actual hostname
    * @return
    */
  final def convertKafkaHostToHostname(kafkaHost: String): String = {
    if (kafkaHost.matches("/\\d+\\.\\d+\\.\\d+\\.\\d+")) {
      try {
        return InetAddress.getByName(kafkaHost.substring(1)).getHostName
      } catch {
        case ex: Throwable => ()
      }
    }
    return kafkaHost
  }

}
