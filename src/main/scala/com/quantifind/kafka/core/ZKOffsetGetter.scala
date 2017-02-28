package com.quantifind.kafka.core

import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.KafkaOffsetInfo
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{BrokerNotAvailableException, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZkUtils}
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.exception.ZkNoNodeException

import scala.collection._
import scala.util.control.NonFatal

/**
  * Kafka offset getter from ZooKeeper storage
  * User: pierre
  * Date: 1/22/14
  */
class ZKOffsetGetter(theZkUtils: ZkUtilsWrapper) extends OffsetGetter {

  override val zkUtils = theZkUtils

  // get the Kafka simple consumer so that we can fetch broker offsets
  protected def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath + "/" + bid) match {
        case (Some(brokerInfoString), _) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case (None, _) =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        error("Could not parse broker info", t)
        None
    }
  }

  override def processPartition(group: String, topic: String, pid: Int): Option[KafkaOffsetInfo] = {
    try {
      val (offset, stat: Stat) = zkUtils.readData(s"${ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      val (owner, _) = zkUtils.readDataMaybeNull(s"${ZkUtils.ConsumersPath}/$group/owners/$topic/$pid")

      zkUtils.getLeaderForPartition(topic, pid) match {
        case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val topicAndPartition = TopicAndPartition(topic, pid)
              val request =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head

              KafkaOffsetInfo(
                group = group,
                topic = topic,
                partition = pid,
                offset = offset.toLong,
                logSize = logSize,
                owner = owner,
                creation = Time.fromMilliseconds(stat.getCtime),
                modified = Time.fromMilliseconds(stat.getMtime))
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  override def getKafkaGroups: Seq[String] = {
    try {
      zkUtils.getChildren(ZkUtils.ConsumersPath)
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }

  override def getTopicList(group: String): List[String] = {
    try {
      zkUtils.getChildren(s"${ZkUtils.ConsumersPath}/$group/offsets").toList
    } catch {
      case _: ZkNoNodeException => List()
    }
  }

  /**
    * Returns a map of topics -> list of consumers, including non-active
    */
  override def getTopicMap: Map[String, Seq[String]] = {
    try {
      zkUtils.getChildren(ZkUtils.ConsumersPath).flatMap {
        group => {
          getTopicList(group).map(topic => topic -> group)
        }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    try {
      zkUtils.getChildren(ZkUtils.ConsumersPath).flatMap {
        group =>
          try {
            zkUtils.getConsumersPerTopic(group, true).keySet.map {
              key =>
                key -> group
            }
          } catch {
            case NonFatal(t) =>
              error(s"could not get consumers for group $group", t)
              Seq()
          }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }
}
