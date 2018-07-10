package com.quantifind.kafka.core

import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util
import java.util.{Collections, Properties}

import com.quantifind.kafka.OffsetGetter.KafkaOffsetInfo
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.kafka.{Node, OffsetGetter}
import com.quantifind.utils.ZkUtilsWrapper
import com.quantifind.utils.Utils.convertKafkaHostToHostname
import com.twitter.util.Time
import kafka.admin.AdminClient
import kafka.common.{KafkaException, OffsetAndMetadata, OffsetMetadata, TopicAndPartition}
import kafka.coordinator.{GroupMetadataManager, GroupOverview, GroupTopicPartition, OffsetKey}
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection._
import scala.collection.JavaConverters._

/**
  * Kafka offset getter from Kafka storage
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter(zkUtilsWrapper: ZkUtilsWrapper, args: OffsetGetterArgs) extends OffsetGetter {

  import KafkaOffsetGetter._

  /**
    * All data must come from Kafka brokers
    */
  override val zkUtils: ZkUtilsWrapper = zkUtilsWrapper

  override def processPartition(group: String, topic: String, partitionId: Int): Option[KafkaOffsetInfo] = {

    val topicPartition = new TopicPartition(topic, partitionId)

    kafkaGroupTopicPartitionToOffsetAndMetadataMap.get(GroupTopicPartition(group, topicPartition)) map { offsetMetaData =>

      val logsize = kafkaTopicPartitionToLogsizeMap(topicPartition)
      val committedOffset = offsetMetaData.offset
      val lag = logsize - committedOffset

      // Get client information if we can find an associated client
      val pontetialClients = kafkaClientGroups.filter(c => c.group == group && c.topicPartitions.contains(topicPartition))
      var clientString: String =
        if (pontetialClients.nonEmpty && !pontetialClients.head.clientId.isEmpty && !pontetialClients.head.clientHost.isEmpty) {
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
    kafkaGroupNameSet.toSeq.sorted
  }

  override def getKafkaTopicList(group: String): List[String] = {
    kafkaTopicGroupSet.filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
  }

  override def getTopicToGroupsMap: Map[String, Seq[String]] = {
    kafkaTopicGroupSet.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
  }

  override def getActiveTopicToGroupsMap: Map[String, Seq[String]] = {
    getTopicToGroupsMap
  }

  override def getKafkaTopics: Seq[String] = {
    kafkaTopicNameToPartitionInfoMap.keys.toSeq.sorted
  }

  override def getKafkaClusterViz: Node = {
    val clusterNodes = kafkaTopicNameToPartitionInfoMap.values.flatMap(partition => {
      partition.head.replicas()
    }).map(node => {
      Node(node.host() + ":" + node.port())
    }).toSet.toSeq.sortWith(_.name < _.name)
    Node("Cluster", clusterNodes)
  }
}

object KafkaOffsetGetter extends Logging {

  val sleepAfterFailureMillis: Long = 5000

  val kafkaMonitorGroupPrefix: String = "kafka-monitor-"
  val kafkaOffsetLogsizeGroup: String = kafkaMonitorGroupPrefix + "logsize"
  val kafkaOffsetCommitsGroup: String = kafkaMonitorGroupPrefix + "commits"
  val kafkaOffsetTopic: String = "__consumer_offsets"

  var kafkaClientGroups: immutable.Set[KafkaClientGroup] = immutable.HashSet()

  var kafkaGroupNameSet: immutable.Set[String] = immutable.HashSet()
  val kafkaGroupTopicPartitionToOffsetAndMetadataMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()

  var kafkaTopicGroupSet: immutable.Set[KafkaTopicGroup] = immutable.HashSet()
  var kafkaTopicNameToPartitionInfoMap: immutable.Map[String, List[PartitionInfo]] = immutable.HashMap()
  val kafkaTopicPartitionToLogsizeMap: concurrent.Map[TopicPartition, Long] = concurrent.TrieMap()
  var kafkaActiveTopicAndPartitionSet: immutable.Set[TopicAndPartition] = immutable.HashSet()


  private def createKafkaConsumerClient(args: OffsetGetterArgs, group: String): KafkaConsumer[Array[Byte], Array[Byte]] = {

    val props: Properties = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
    if(args.kafkaSecurityProtocol.equalsIgnoreCase("SSL")){
      props.put("ssl.endpoint.identification.algorithm", "")
      props.put("ssl.truststore.location",args.sslTruststoreLocation )
      props.put("ssl.truststore.password",args.sslTruststorePWD )
    }
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  private def createAdminClient(args: OffsetGetterArgs): AdminClient = {

    var adminClient: AdminClient = null

    val props: Properties = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
    if(args.kafkaSecurityProtocol.equalsIgnoreCase("SSL")){
      props.put("ssl.endpoint.identification.algorithm", "")
      props.put("ssl.truststore.location",args.sslTruststoreLocation )
      props.put("ssl.truststore.password",args.sslTruststorePWD )
    }
    while (adminClient == null) {
      try {
        info("Creating new Kafka admin client")
        adminClient = AdminClient.create(props)
      }
      catch {
        case e: Throwable =>
          error("Exception during creating Kafka AdminClient, waiting and retrying", e)
          if (adminClient != null) {
            try {
              adminClient.close()
            } catch {
              case ex: Throwable => ()
            }
            adminClient = null
            Thread.sleep(sleepAfterFailureMillis)
          }
      }
    }

    info("Created Kafka AdminClient " + adminClient)
    adminClient
  }

  /**
    * Attempts to parse a kafka message as an offset message
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
        Option.empty
      } else {
        // Match on the key to see if the message is an offset message
        GroupMetadataManager.readMessageKey(ByteBuffer.wrap(message.key)) match {
          // This is the type we are looking for
          case offsetKey: OffsetKey =>
            val groupTopicPartition: GroupTopicPartition = offsetKey.key
            val messageBody: Array[Byte] = message.value()
            val offsetAndMetadata: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(messageBody))
            Option(groupTopicPartition, offsetAndMetadata)
          case _ =>
            info("Ignoring non-offset message")
            Option.empty
        }
      }
    } catch {
      case malformedEx@(_: BufferUnderflowException | _: KafkaException) =>
        error("The message was malformed and does not conform to a type of (BaseKey, OffsetAndMetadata), ignoring the message", malformedEx)
        Option.empty
      case e: Throwable =>
        val errorMsg = String.format(".")
        error("An unhandled exception was thrown while attempting to determine the validity of a message as an offset message, ignoring the message", e)
        Option.empty
    }
  }

  def startCommittedOffsetListener(args: OffsetGetterArgs): Unit = {

    var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {
      try {
        while (offsetConsumer == null) {
          logger.info("Creating new Kafka client to get consumer group committed offsets")
          offsetConsumer = createKafkaConsumerClient(args, kafkaOffsetCommitsGroup + "-" + System.currentTimeMillis / 1000)
          offsetConsumer.subscribe(Collections.singletonList(kafkaOffsetTopic))
        }

        val messages: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(1000)
        val messageIterator = messages.iterator()

        val updatedGroupTopicPartitions = mutable.Set[String]()
        while (messageIterator.hasNext) {

          val message: ConsumerRecord[Array[Byte], Array[Byte]] = messageIterator.next()
          val offsetMessageOption: Option[(GroupTopicPartition, OffsetAndMetadata)] = tryParseOffsetMessage(message)

          if (offsetMessageOption.isDefined) {
            // Deal with the offset message
            val messageOffsetMap: (GroupTopicPartition, OffsetAndMetadata) = offsetMessageOption.get

            val groupTopicPartition: GroupTopicPartition = messageOffsetMap._1
            val offsetAndMetadata: OffsetAndMetadata = messageOffsetMap._2

            kafkaGroupTopicPartitionToOffsetAndMetadataMap += messageOffsetMap

            updatedGroupTopicPartitions += groupTopicPartition.group + " / " + groupTopicPartition.topicPartition.topic + " / " + groupTopicPartition.topicPartition.partition + " -> " + offsetAndMetadata.offset
          }
        }

        updatedGroupTopicPartitions.foreach((s) => {
          info(s"Updating commited offset $s")
        })
      } catch {
        case e: Throwable =>
          error("An unhandled exception was thrown while reading messages from the committed offsets topic, waiting and retrying", e)
          if (offsetConsumer != null) {
            try {
              offsetConsumer.close()
            } catch {
              case ex: Throwable => ()
            }
            offsetConsumer = null
            Thread.sleep(sleepAfterFailureMillis)
          }
      }
    }
  }

  def startAdminClient(args: OffsetGetterArgs): Unit = {

    val sleepDurationMillis: Int = 30000
    var adminClient: AdminClient = null

    while (true) {
      try {

        while (adminClient == null) {
          logger.info("Creating new Kafka admin client")
          adminClient = createAdminClient(args)
        }

        val currentTopicAndGroupSet: mutable.Set[KafkaTopicGroup] = mutable.HashSet()
        val currentClientSet: mutable.Set[KafkaClientGroup] = mutable.HashSet()
        val currentActiveTopicPartitionSet: mutable.HashSet[TopicAndPartition] = mutable.HashSet()
        val groupOverviewList = adminClient.listAllConsumerGroupsFlattened().filter(c => !c.groupId.startsWith(kafkaMonitorGroupPrefix))

        groupOverviewList.foreach((groupOverview: GroupOverview) => {

          val groupId = groupOverview.groupId
          val offsets: Map[TopicPartition, Long] = adminClient.listGroupOffsets(groupId)
          val consumerGroupSummary = adminClient.describeConsumerGroup(groupId)

          if (consumerGroupSummary.state == "Stable") {
            val consumers: Seq[AdminClient#ConsumerSummary] = adminClient.describeConsumerGroup(groupId).consumers.get
            consumers.foreach(
              consumerSummary => {

                val clientId = consumerSummary.clientId
                val clientHost = convertKafkaHostToHostname(consumerSummary.host)
                val topicPartitions: Seq[TopicPartition] = consumerSummary.assignment

                topicPartitions.foreach(
                  topicPartition => {
                    currentActiveTopicPartitionSet.add(TopicAndPartition(topicPartition.topic(), topicPartition.partition()))
                    currentTopicAndGroupSet.add(KafkaTopicGroup(topicPartition.topic(), groupId))
                  })

                currentClientSet += KafkaClientGroup(groupId, clientId, clientHost, topicPartitions.toSet)
              })
            // group can be in "Empty" state
          } else {
            offsets.foreach {
              case (topicPartition, offset) =>
                kafkaGroupTopicPartitionToOffsetAndMetadataMap.put(
                  GroupTopicPartition(groupId, topicPartition),
                  OffsetAndMetadata(OffsetMetadata(offset = offset), commitTimestamp = 0, expireTimestamp = 0)
                )
                kafkaTopicPartitionToLogsizeMap.put(topicPartition, 0)

                // adding these fake consumers in order to show empty entries in any case
                currentActiveTopicPartitionSet.add(TopicAndPartition(topicPartition.topic, topicPartition.partition))
                currentTopicAndGroupSet.add(KafkaTopicGroup(topicPartition.topic, groupId))
            }
            currentClientSet.add(KafkaClientGroup(groupId, "", "", offsets.keys.toSet))
          }
        })

        kafkaGroupNameSet = (for (x <- groupOverviewList) yield x.groupId) (collection.breakOut).toSet
        kafkaActiveTopicAndPartitionSet = currentActiveTopicPartitionSet.toSet
        kafkaClientGroups = currentClientSet.toSet
        kafkaTopicGroupSet = currentTopicAndGroupSet.toSet

        Thread.sleep(sleepDurationMillis)
      } catch {
        case e: Throwable =>
          error("Kafka AdminClient processing aborted due to an unexpected exception, waiting and rertrying", e)
          if (null != adminClient) {
            try {
              adminClient.close()
            } catch {
              case ex: Throwable => ()
            }
            adminClient = null
            Thread.sleep(sleepAfterFailureMillis)
          }
      }
    }
  }

  def startLogsizeGetter(args: OffsetGetterArgs): Unit = {

    val sleepOnDataRetrieval: Int = 10000
    var logsizeKafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {
      try {

        while (logsizeKafkaConsumer == null) {
          logger.info("Creating new Kafka client to get topic logsize")
          logsizeKafkaConsumer = createKafkaConsumerClient(args, kafkaOffsetLogsizeGroup + "-" + System.currentTimeMillis / 1000)
        }

        kafkaTopicNameToPartitionInfoMap =
          JavaConversions.mapAsScalaMap(logsizeKafkaConsumer.listTopics()).toMap map {
            case (topic, partitionInfos: util.List[PartitionInfo]) => (topic, partitionInfos.asScala.toList)
          }

        val distinctPartitionInfo: Seq[PartitionInfo] = kafkaTopicNameToPartitionInfoMap.values.flatten(
          listPartitionInfo => listPartitionInfo
        ).toSeq

        // Iterate over each distinct PartitionInfo
        distinctPartitionInfo.foreach(partitionInfo => {
          // Get the logsize (end offset value) for each TopicPartition
          val topicPartition: TopicPartition = new TopicPartition(partitionInfo.topic, partitionInfo.partition)
          logsizeKafkaConsumer.assign(Collections.singletonList(topicPartition))
          logsizeKafkaConsumer.seekToEnd(Collections.singletonList(topicPartition))
          val logsize: Long = logsizeKafkaConsumer.position(topicPartition)

          info(s"Updating logsize topic ${partitionInfo.topic()} / partition ${partitionInfo.partition()} -> $logsize")
          kafkaTopicPartitionToLogsizeMap.put(topicPartition, logsize)
        })

        Thread.sleep(sleepOnDataRetrieval)
      }
      catch {
        case e: Throwable =>
          error("The Kafka Client reading topic/partition logsizes has thrown an unhandled exception, waiting and restarting", e)
          if (logsizeKafkaConsumer != null) {
            try {
              logsizeKafkaConsumer.close()
            } catch {
              case ex: Throwable => ()
            }
            logsizeKafkaConsumer = null
            Thread.sleep(sleepAfterFailureMillis)
          }
      }
    }
  }
}

case class KafkaTopicGroup(topic: String, group: String)

case class KafkaClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicPartition])
