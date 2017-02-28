package com.quantifind.kafka.core

import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util
import java.util.{Arrays, Properties}

import com.quantifind.kafka.OffsetGetter.KafkaOffsetInfo
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.kafka.{Node, OffsetGetter}
import com.quantifind.utils.{ZkUtilsWrapper}
import com.quantifind.utils.Utils.convertKafkaHostToHostname
import com.twitter.util.Time
import kafka.admin.AdminClient
import kafka.common.{KafkaException, OffsetAndMetadata, TopicAndPartition, OffsetMetadata}
import kafka.coordinator._
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection._

/**
  * Kafka offset getter from Kafka storage
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter(zkUtilsWrapper: ZkUtilsWrapper, args: OffsetGetterArgs) extends OffsetGetter {

  import KafkaOffsetGetter._

  /**
    * All data must come from Kafka brokers
    */
  override val zkUtils = zkUtilsWrapper

  override def processPartition(group: String, topic: String, partitionId: Int): Option[KafkaOffsetInfo] = {

    val topicPartition = new TopicPartition(topic, partitionId)

    kafkaGroupPartitionToOffsetMetadataMap.get(GroupTopicPartition(group, topicPartition)) map { offsetMetaData =>

      val logsize = kafkaTopicPartitionToLogsizeMap.get(topicPartition).get
      val committedOffset = offsetMetaData.offset
      val lag = logsize - committedOffset

      // Get client information if we can find an associated client
      val pontetialClients = kafkaClients.filter(c => (c.group == group && c.topicPartitions.contains(topicPartition)))
      var clientString: String =
        if (!pontetialClients.isEmpty && !pontetialClients.head.clientId.isEmpty && !pontetialClients.head.clientHost.isEmpty) {
          val client = pontetialClients.head
          client.clientId + " / " + client.clientHost
        } else {
          ""
        }

      KafkaOffsetInfo(
        group = group,
        topic = topic,
        partition = partitionId,
        offset = committedOffset,
        logSize = logsize,
        owner = Option(clientString),
        Time.Undefined,
        Time.Undefined
      )
    }
  }

  override def getKafkaGroups: Seq[String] = {
    kafkaGroups.toSeq.sorted
  }

  override def getKafkaTopicList(group: String): List[String] = {
    kafkaTopicGroups.filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
  }

  override def getTopicToGroupsMap: Map[String, scala.Seq[String]] = {
    kafkaTopicGroups.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
  }

  override def getActiveTopicToGroupsMap: Map[String, Seq[String]] = {
    getTopicToGroupsMap
  }

  override def getKafkaTopics: Seq[String] = {
    kafkaTopicToPartitionInfosMap.keys.toSeq.sorted
  }

  override def getKafkaClusterViz: Node = {
    val clusterNodes = kafkaTopicToPartitionInfosMap.values.map(partition => {
      val leader = partition.get(0).leader()
      Node(leader.host() + ":" + leader.port())
    }).toSet.toSeq.sortWith(_.name < _.name)
    Node("Cluster", clusterNodes)
  }
}

object KafkaOffsetGetter extends Logging {

  val kafkaOffsetLogsizeGroup = "kafka-monitor-logsize"
  val kafkaOffsetCommitsGroup = "kafka-monitor-commits"
  val kafkaOffsetTopic = "__consumer_offsets"

  /**
    * Cached state of Kafka cluster
    */
  val kafkaGroupPartitionToOffsetMetadataMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()
  val kafkaTopicPartitionToLogsizeMap: concurrent.Map[TopicPartition, Long] = concurrent.TrieMap()
  var kafkaGroups: immutable.Set[String] = immutable.HashSet()
  var kafkaActiveTopicPartitions: immutable.Set[TopicAndPartition] = immutable.HashSet()
  var kafkaClients: immutable.Set[KafkaClientGroup] = immutable.HashSet()
  var kafkaTopicGroups: immutable.Set[KafkaTopicGroup] = immutable.HashSet()
  var kafkaTopicToPartitionInfosMap: immutable.Map[String, util.List[PartitionInfo]] = immutable.HashMap()

  private def createNewKafkaConsumer(args: OffsetGetterArgs, group: String): KafkaConsumer[Array[Byte], Array[Byte]] = {

    val props: Properties = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  private def createNewAdminClient(args: OffsetGetterArgs): AdminClient = {

    val sleepAfterFailedAdminClientConnect: Int = 30000
    var adminClient: AdminClient = null

    val props: Properties = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)

    while (adminClient == null) {
      try {
        info("Creating new Kafka AdminClient to get consumer and group info");
        adminClient = AdminClient.create(props)
      }

      catch {
        case e: Throwable =>
          error("Exception during creating Kafka AdminClient, retrying", e)
          if (adminClient != null) {
            try {
              adminClient.close()
            } catch {
              case ex: Throwable => ()
            }
            adminClient = null
          }
      }
    }

    info("Created Kafka AdminClient " + adminClient)
    return adminClient
  }

  /**
    * Attempts to parse a kafka message as an offset message.
    *
    * @author Robert Casey (rcasey212@gmail.com)
    * @param message message retrieved from the kafka client's poll() method
    * @return key-value of GroupTopicPartition and OffsetAndMetadata if the message was a valid offset message,
    *         otherwise None
    */
  def tryParseOffsetMessage(message: ConsumerRecord[Array[Byte], Array[Byte]]): Option[(GroupTopicPartition, OffsetAndMetadata)] = {

    try {
      // If the message has a null key or value, there is nothing that can be done
      if (message.key == null || message.value == null) {
        info("Ignoring message with a null key or null value")
        return None
      }

      // Match on the key to see if the message is an offset message
      val baseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(message.key))
      baseKey match {
        // This is the type we are looking for
        case offsetKey: OffsetKey =>
          val messageBody: Array[Byte] = message.value()
          val groupTopicPartition: GroupTopicPartition = offsetKey.key
          val offsetAndMetadata: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(messageBody))
          return Option(groupTopicPartition, offsetAndMetadata)

        case _ =>
          info("Ignoring non-offset message")
          return None
      }
    } catch {
      case malformedEx@(_: BufferUnderflowException | _: KafkaException) =>
        error("The message was malformed and does not conform to a type of (BaseKey, OffsetAndMetadata), ignoring the message", malformedEx)
        return None

      case e: Throwable =>
        val errorMsg = String.format(".")
        error("An unhandled exception was thrown while attempting to determine the validity of a message as an offset message, ignoring the message", e)
        return None
    }
  }

  def startCommittedOffsetListener(args: OffsetGetterArgs) = {

    var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {

      try {
        if (offsetConsumer == null) {
          logger.info("Creating new Kafka client to get consumer group committed offsets")
          offsetConsumer = createNewKafkaConsumer(args, kafkaOffsetCommitsGroup)
          offsetConsumer.subscribe(Arrays.asList(kafkaOffsetTopic))
        }

        val messages: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(1000)
        val messageIterator = messages.iterator()

        while (messageIterator.hasNext()) {

          val message: ConsumerRecord[Array[Byte], Array[Byte]] = messageIterator.next()
          val offsetMessage: Option[(GroupTopicPartition, OffsetAndMetadata)] = tryParseOffsetMessage(message)

          if (offsetMessage.isDefined) {

            // Deal with the offset message
            val messageOffsetMap: (GroupTopicPartition, OffsetAndMetadata) = offsetMessage.get
            val groupTopicPartition: GroupTopicPartition = messageOffsetMap._1
            val offsetAndMetadata: OffsetAndMetadata = messageOffsetMap._2
            kafkaGroupPartitionToOffsetMetadataMap += messageOffsetMap

            {
              val group = groupTopicPartition.group
              val topic = groupTopicPartition.topicPartition.topic
              val partition = groupTopicPartition.topicPartition.partition
              val offset = offsetAndMetadata.offset

              debug(s"Update group: $group, topic: $topic, partition: $partition, offset: $offset")
            }

          }
        }
      } catch {
        case e: Throwable => {
          error("An unhandled exception was thrown while reading messages from the committed offsets topic", e)
          if (offsetConsumer != null) {
            try {
              offsetConsumer.close()
            } catch {
              case ex: Throwable => ()
            }
            offsetConsumer = null
          }
        }
      }
    }
  }

  def startAdminClient(args: OffsetGetterArgs) = {

    val sleepDurationMillis: Int = 30000
    val awaitForResults: Int = 30000
    var adminClient: AdminClient = null

    while (true) {
      try {
        while (adminClient == null) {
          adminClient = createNewAdminClient(args)
        }

        val currentTopicAndGroups: mutable.Set[KafkaTopicGroup] = mutable.HashSet()
        val currentClients: mutable.Set[KafkaClientGroup] = mutable.HashSet()
        val currentActiveTopicPartitions: mutable.HashSet[TopicAndPartition] = mutable.HashSet()

        val groupOverviews = adminClient.listAllConsumerGroupsFlattened().filter(c => c.groupId != kafkaOffsetCommitsGroup)

        groupOverviews.foreach((groupOverview: GroupOverview) => {
          val groupId = groupOverview.groupId;
          val offsets: Map[TopicPartition, Long] = adminClient.listGroupOffsets(groupId)
          val consumerGroupSummary = adminClient.describeConsumerGroup(groupId)

          if (consumerGroupSummary.state == "Stable") {
            val consumers: Seq[AdminClient#ConsumerSummary] = adminClient.describeConsumerGroup(groupId).consumers.get
            consumers.foreach((consumerSummary) => {

              val clientId = consumerSummary.clientId
              val clientHost = convertKafkaHostToHostname(consumerSummary.host)

              val topicPartitions: Seq[TopicPartition] = consumerSummary.assignment

              topicPartitions.foreach((topicPartition) => {
                currentActiveTopicPartitions += TopicAndPartition(topicPartition.topic(), topicPartition.partition())
                currentTopicAndGroups += KafkaTopicGroup(topicPartition.topic(), groupId)
              })

              currentClients += KafkaClientGroup(groupId, clientId, clientHost, topicPartitions.toSet)
            })

          } else {
            offsets.foreach {
              case (topicPartition, offset) => {
                kafkaGroupPartitionToOffsetMetadataMap.put(
                  new GroupTopicPartition(groupId, topicPartition),
                  new OffsetAndMetadata(new OffsetMetadata(offset = offset), commitTimestamp = 0, expireTimestamp = 0)
                )
                kafkaTopicPartitionToLogsizeMap.put(topicPartition, 0)

                // adding these fake consumers in order to show empty entries in any case
                currentActiveTopicPartitions += TopicAndPartition(topicPartition.topic(), topicPartition.partition())
                currentTopicAndGroups += KafkaTopicGroup(topicPartition.topic(), groupId)
              }
            }
            currentClients += KafkaClientGroup(groupId, "", "", offsets.keys.toSet)
          }
        })

        kafkaGroups = (for (x <- groupOverviews) yield x.groupId) (collection.breakOut).toSet
        kafkaActiveTopicPartitions = currentActiveTopicPartitions.toSet
        kafkaClients = currentClients.toSet
        kafkaTopicGroups = currentTopicAndGroups.toSet

        Thread.sleep(sleepDurationMillis)
      } catch {
        case ex: java.util.concurrent.TimeoutException => {
          warn("The AdminClient timed out, closing and restarting")
          if (adminClient != null) {
            try {
              adminClient.close()
            } catch {
              case ex: Throwable => ()
            }
            adminClient = null
          }
        }
        case e: Throwable =>
          error("Kafka AdminClient processing aborted due to an unexpected exception, closing and restarting", e)
          if (null != adminClient) {
            try {
              adminClient.close()
            } catch {
              case ex: Throwable => ()
            }
            adminClient = null
          }
      }
    }
  }

  def startLogEndOffsetGetter(args: OffsetGetterArgs) = {

    val sleepOnDataRetrieval: Int = 10000
    val sleepOnError: Int = 30000
    var logsizeKafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {
      try {
        while (logsizeKafkaConsumer == null) {
          logsizeKafkaConsumer = createNewKafkaConsumer(args, kafkaOffsetLogsizeGroup)
        }

        kafkaTopicToPartitionInfosMap = JavaConversions.mapAsScalaMap(logsizeKafkaConsumer.listTopics()).toMap
        val distinctPartitionInfo: Seq[PartitionInfo] = (kafkaTopicToPartitionInfosMap.values).flatten(listPartitionInfo => JavaConversions.asScalaBuffer(listPartitionInfo)).toSeq

        // Iterate over each distinct PartitionInfo
        distinctPartitionInfo.foreach(partitionInfo => {
          // Get the logsize (end offset value) for each TopicPartition
          val topicPartition: TopicPartition = new TopicPartition(partitionInfo.topic, partitionInfo.partition)
          logsizeKafkaConsumer.assign(Arrays.asList(topicPartition))
          logsizeKafkaConsumer.seekToEnd(Arrays.asList(topicPartition))
          val logsize: Long = logsizeKafkaConsumer.position(topicPartition)

          kafkaTopicPartitionToLogsizeMap.put(topicPartition, logsize)
        })

        Thread.sleep(sleepOnDataRetrieval)
      }
      catch {
        case e: Throwable => {
          error("The Kafka Client reading topic/partition logsizes has thrown an unhandled exception, closing and restarting", e)
          if (logsizeKafkaConsumer != null) {
            try {
              logsizeKafkaConsumer.close()
            } catch {
              case ex: Throwable => ()
            }
            logsizeKafkaConsumer = null
          }
        }
      }
    }
  }
}

case class KafkaTopicGroup(topic: String, group: String)

case class KafkaClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicPartition])
