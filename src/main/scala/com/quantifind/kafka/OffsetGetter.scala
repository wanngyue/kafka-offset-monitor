package com.quantifind.kafka

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.quantifind.kafka.OffsetGetter.{KafkaBrokerInfo, KafkaGroupInfo, KafkaOffsetInfo}
import com.quantifind.kafka.core._
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.consumer.{ConsumerConnector, SimpleConsumer}
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.security.JaasUtils

import scala.collection._
import scala.util.control.NonFatal

case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class TopicDetailsWrapper(consumers: TopicDetails)

case class TopicAndConsumersDetails(active: Seq[KafkaGroupInfo], inactive: Seq[KafkaGroupInfo])

case class ConsumerDetail(name: String)

/**
  * OffsetGetter trait that must be implemented specifically for each storage
  */
trait OffsetGetter extends Logging {

  val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  def zkUtils: ZkUtilsWrapper

  /**
    * Get list of Kafka groups ever existed
    *
    * @return
    */
  def getKafkaGroups: Seq[String]

  /**
    * Get list of Kafka topics ever existed for the given consumer group
    *
    * @param group
    * @return
    */
  def getKafkaTopicList(group: String): List[String]

  def getTopicToGroupsMap: Map[String, Seq[String]]

  def getActiveTopicToGroupsMap: Map[String, Seq[String]]

  /**
    * Get information about a consumer group and the topics it consumes
    */
  def getKafkaGroupInfo(group: String, topics: Seq[String] = Seq()): KafkaGroupInfo = {

    def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[KafkaOffsetInfo] = {

      def processTopic(group: String, topic: String): Seq[KafkaOffsetInfo] = {
        val topicToPartitionsMap = zkUtils.getPartitionsForTopics(Seq(topic))
        for {
          partitions <- topicToPartitionsMap.get(topic).toSeq
          partitionId <- partitions.sorted
          info <- processPartition(group, topic, partitionId)
        } yield info
      }

      val topicList =
        if (topics.isEmpty) {
          getKafkaTopicList(group)
        } else {
          topics
        }

      topicList.sorted.flatMap(processTopic(group, _))
    }

    def brokerInfo(): Seq[KafkaBrokerInfo] = {
      consumerMap.toSeq map {
        case (brokerId, consumer) => new KafkaBrokerInfo(id = brokerId, host = consumer.get.host, port = consumer.get.port)
      }
    }

    val kafkaOffsetsInfo = offsetInfo(group, topics)
    val kafkaBrokersInfo = brokerInfo()

    KafkaGroupInfo(
      group = group,
      brokers = kafkaBrokersInfo,
      offsets = kafkaOffsetsInfo
    )
  }

  /**
    * Get list of all topics
    */
  def getKafkaTopics: Seq[String] = {
    try {
      zkUtils.getChildren(ZkUtils.BrokerTopicsPath).sortWith(_ < _)
    } catch {
      case NonFatal(t) =>
        error(s"could not get topics because of ${t.getMessage}", t)
        Seq()
    }
  }

  def getKafkaClusterViz: Node = {
    val clusterNodes = zkUtils.getAllBrokersInCluster().map((broker) => {
      Node(broker.toString(), Seq())
    })
    Node("KafkaCluster", clusterNodes)
  }

  /**
    * Returns details for a given topic such as the consumers pulling off of it
    */
  def getKafkaTopicDetails(topic: String): TopicDetails = {
    val topicMap = getActiveTopicToGroupsMap

    if (topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }))
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  /**
    * Returns details for a given topic such as the active consumers pulling off of it
    * and for each of the active consumers it will return the consumer data
    */
  def getKafkaTopicAndConsumersDetails(topic: String): TopicAndConsumersDetails = {
    val topicToGroupsMap = getTopicToGroupsMap
    val activeTopicToGroupsMap = getActiveTopicToGroupsMap

    def mapConsumersToKafkaInfo(consumers: Seq[String], topic: String): Seq[KafkaGroupInfo] =
      consumers.map(getKafkaGroupInfo(_, Seq(topic)))

    val activeConsumers =
      if (activeTopicToGroupsMap.contains(topic)) {
        mapConsumersToKafkaInfo(activeTopicToGroupsMap(topic), topic)
      } else {
        Seq()
      }

    val inactiveConsumers =
      if (!activeTopicToGroupsMap.contains(topic) && topicToGroupsMap.contains(topic)) {
        mapConsumersToKafkaInfo(topicToGroupsMap(topic), topic)
      } else if (activeTopicToGroupsMap.contains(topic) && topicToGroupsMap.contains(topic)) {
        mapConsumersToKafkaInfo(topicToGroupsMap(topic).diff(activeTopicToGroupsMap(topic)), topic)
      } else {
        Seq()
      }

    TopicAndConsumersDetails(activeConsumers, inactiveConsumers)
  }

  def getKafkaActiveTopics: Node = {
    val topicMap = getActiveTopicToGroupsMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def processPartition(group: String, topic: String, partitionId: Int): Option[KafkaOffsetInfo]

}

object OffsetGetter {

  val kafkaOffsetListenerStarted: AtomicBoolean = new AtomicBoolean(false)
  var zkUtils: ZkUtilsWrapper = null
  var consumerConnector: ConsumerConnector = null
  var newKafkaConsumer: KafkaConsumer[String, String] = null

  def createZkUtils(args: OffsetGetterArgs): ZkUtils = {
    ZkUtils(
      args.zk,
      args.zkSessionTimeout.toMillis.toInt,
      args.zkConnectionTimeout.toMillis.toInt,
      JaasUtils.isZkSecurityEnabled()
    )
  }

  def getInstance(args: OffsetGetterArgs): OffsetGetter = {

    // create and initialize resources only once
    if (kafkaOffsetListenerStarted.compareAndSet(false, true)) {
      // needed for all OffsetGetters
      zkUtils = new ZkUtilsWrapper(createZkUtils(args))

      // specific to kafka storage
      if (args.offsetStorage.toLowerCase.equals("kafka")) {
        val adminClientExecutor = Executors.newSingleThreadExecutor()
        adminClientExecutor.submit(new Runnable() {
          def run() = KafkaOffsetGetter.startAdminClient(args)
        })

        val logEndOffsetExecutor = Executors.newSingleThreadExecutor()
        logEndOffsetExecutor.submit(new Runnable() {
          def run() = KafkaOffsetGetter.startLogEndOffsetGetter(args)
        })

        val committedOffsetExecutor = Executors.newSingleThreadExecutor()
        committedOffsetExecutor.submit(new Runnable() {
          def run() = KafkaOffsetGetter.startCommittedOffsetListener(args)
        })
      }
    }

    args.offsetStorage.toLowerCase match {
      case "kafka" =>
        new KafkaOffsetGetter(zkUtils, args)
      case _ =>
        new ZKOffsetGetter(zkUtils)
    }
  }

  case class KafkaGroupInfo(group: String, brokers: Seq[KafkaBrokerInfo], offsets: Seq[KafkaOffsetInfo])

  case class KafkaBrokerInfo(id: Int, host: String, port: Int)

  case class KafkaOffsetInfo(group: String, topic: String, partition: Int, offset: Long, logSize: Long, owner: Option[String], creation: Time, modified: Time) {
    val lag = logSize - offset
  }


}