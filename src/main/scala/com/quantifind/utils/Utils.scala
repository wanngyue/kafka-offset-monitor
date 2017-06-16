package com.quantifind.utils

import java.net.InetAddress

import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, immutable, mutable}
import scala.util.{Failure, Success, Try}

/**
  * Basic utils
  *
  * @author xorlev
  */

object Utils {

  /**
    * Instead of giving the hostname of the consumer, Kafka broker sometimes gives "/ip4-address" value,
    * try to replace by the actual hostname
    *
    * @return
    */
  val kafkaHostToHostnameMap: TrieMap[String, String] = concurrent.TrieMap[String, String]()

  final def convertKafkaHostToHostname(kafkaHost: String): String = {
    kafkaHostToHostnameMap.get(kafkaHost) match {
      case cachedResult: Some[String] => cachedResult.get
      case _ =>
        if (kafkaHost.matches("/\\d+\\.\\d+\\.\\d+\\.\\d+")) {
          try {
            val resolvedKafkaHost: String = InetAddress.getByName(kafkaHost.substring(1)).getHostName
            kafkaHostToHostnameMap += (kafkaHost -> resolvedKafkaHost)
            resolvedKafkaHost
          } catch {
            case ex: Throwable => kafkaHost
          }
        } else {
          kafkaHost
        }
    }
  }

  // Returning T, throwing the exception on failure
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try {
      fn
    } match {
      case Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }
}
