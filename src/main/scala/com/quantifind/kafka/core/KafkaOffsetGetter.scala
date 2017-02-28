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
import scala.concurrent.{Await, Future, duration}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Kafka offset getter from Kafka storage
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter(zkUtilsWrapper: ZkUtilsWrapper, args: OffsetGetterArgs) extends OffsetGetter {

  import KafkaOffsetGetter._

  /**
    * TODO: We will get all data from the Kafka broker in this class.
    * This is here simply to satisfy the OffsetGetter dependency until it can be refactored
    */
  override val zkUtils = zkUtilsWrapper

  override def processPartition(group: String, topic: String, partitionId: Int): Option[KafkaOffsetInfo] = {

    val topicPartition = new TopicPartition(topic, partitionId)

    committedOffsetMap.get(GroupTopicPartition(group, topicPartition)) map { offsetMetaData =>

      val logEndOffset: Long = logEndOffsetsMap.get(topicPartition).get
      val committedOffset: Long = offsetMetaData.offset
      val lag: Long = logEndOffset - committedOffset

      // Get client information if we can find an associated client
      var clientString: Option[String] = Option("")
      val filteredClients = clients.filter(c => (c.group == group && c.topicPartitions.contains(topicPartition)))
      if (!filteredClients.isEmpty) {
        val client: KafkaClientGroup = filteredClients.head
        if (!client.clientId.isEmpty && !client.clientHost.isEmpty) {
          clientString = Option(client.clientId + " / " + client.clientHost)
        }
      }

      KafkaOffsetInfo(
        group = group,
        topic = topic,
        partition = partitionId,
        offset = committedOffset,
        logSize = logEndOffset,
        owner = clientString,
        Time.Undefined,
        Time.Undefined
      )
    }
  }

  override def getKafkaGroups: Seq[String] = {
    groups.toSeq.sorted
  }

  override def getKafkaTopicList(group: String): List[String] = {
    topicAndGroups.filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
  }

  override def getTopicToGroupsMap: Map[String, scala.Seq[String]] = {
    topicAndGroups.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
  }

  override def getActiveTopicToGroupsMap: Map[String, Seq[String]] = {
    getTopicToGroupsMap
  }

  override def getKafkaTopics: Seq[String] = {
    topicPartitionsMap.keys.toSeq.sorted
  }

  override def getKafkaClusterViz: Node = {
    val clusterNodes = topicPartitionsMap.values.map(partition => {
      Node(partition.get(0).leader().host() + ":" + partition.get(0).leader().port(), Seq())
    }).toSet.toSeq.sortWith(_.name < _.name)
    Node("KafkaCluster", clusterNodes)
  }
}

object KafkaOffsetGetter extends Logging {

  val kafkaOffsetMonitorGroup: String = "kafka-offset-monitor-consumer"
  val kafkaOffsetTopic = "__consumer_offsets"

  val committedOffsetMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()
  val logEndOffsetsMap: concurrent.Map[TopicPartition, Long] = concurrent.TrieMap()

  // Swap the object on update
  var groups: immutable.Set[String] = immutable.HashSet()
  var activeTopicPartitions: immutable.Set[TopicAndPartition] = immutable.HashSet()
  var clients: immutable.Set[KafkaClientGroup] = immutable.HashSet()
  var topicAndGroups: immutable.Set[KafkaTopicGroup] = immutable.HashSet()
  var topicPartitionsMap: immutable.Map[String, util.List[PartitionInfo]] = immutable.HashMap()

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
          if (adminClient != null) {
            try {
              adminClient.close()
            } catch {
              case ex: Throwable => ()
            }
            adminClient = null
          }
          error("Error creating an AdminClient, will attempt to re-create in %d seconds".format(sleepAfterFailedAdminClientConnect), e)
          Thread.sleep(sleepAfterFailedAdminClientConnect)
      }
    }

    info("Created admin client: " + adminClient)
    adminClient
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

      val baseKey: BaseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(message.key))

      // Match on the key to see if the message is an offset message
      baseKey match {
        // This is the type we are looking for
        case b: OffsetKey =>
          val messageBody: Array[Byte] = message.value()
          val gtp: GroupTopicPartition = b.key
          val offsetAndMetadata: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(messageBody))
          return Option(gtp, offsetAndMetadata)

        // Return None for all non-offset messages
        case _ =>
          info("Ignoring non-offset message.")
          return None
      }
    } catch {
      case malformedEx@(_: BufferUnderflowException | _: KafkaException) =>
        val errorMsg = String.format("The message was malformed and does not conform to a type of (BaseKey, OffsetAndMetadata. Ignoring this message.")
        error(errorMsg, malformedEx)
        return None

      case e: Throwable =>
        val errorMsg = String.format("An unhandled exception was thrown while attempting to determine the validity of a message as an offset message. This message will be ignored.")
        error(errorMsg, e)
        return None
    }
  }

  def startCommittedOffsetListener(args: OffsetGetterArgs) = {

    var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {

      try {
        if (null == offsetConsumer) {
          logger.info("Creating new Kafka Client to get consumer group committed offsets")
          offsetConsumer = createNewKafkaConsumer(args, kafkaOffsetMonitorGroup)
          offsetConsumer.subscribe(Arrays.asList(kafkaOffsetTopic))
        }

        val messages: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(500)
        val messageIterator = messages.iterator()

        while (messageIterator.hasNext()) {

          val message: ConsumerRecord[Array[Byte], Array[Byte]] = messageIterator.next()
          val offsetMessage: Option[(GroupTopicPartition, OffsetAndMetadata)] = tryParseOffsetMessage(message)

          if (offsetMessage.isDefined) {

            // Deal with the offset message
            val messageOffsetMap: (GroupTopicPartition, OffsetAndMetadata) = offsetMessage.get
            val gtp: GroupTopicPartition = messageOffsetMap._1
            val offsetAndMetadata: OffsetAndMetadata = messageOffsetMap._2

            // Get current offset for topic-partition
            val existingCommittedOffsetMap: Option[OffsetAndMetadata] = committedOffsetMap.get(gtp)

            // Update committed offset only if the new message brings a change in offset for the topic-partition:
            //   a changed offset for an existing topic-partition, or a new topic-partition
            if (!existingCommittedOffsetMap.isDefined || existingCommittedOffsetMap.get.offset != offsetAndMetadata.offset) {

              val group = gtp.group
              val topic = gtp.topicPartition.topic
              val partition: Long = gtp.topicPartition.partition
              val offset: Long = offsetAndMetadata.offset

              info(s"Updating committed offset: g:$group,t:$topic,p:$partition: $offset")
              committedOffsetMap += messageOffsetMap
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
        if (adminClient == null) {
          adminClient = createNewAdminClient(args)
        }

        lazy val f = Future {
          try {
            val currentTopicAndGroups: mutable.Set[KafkaTopicGroup] = mutable.HashSet()
            val currentClients: mutable.Set[KafkaClientGroup] = mutable.HashSet()
            val currentActiveTopicPartitions: mutable.HashSet[TopicAndPartition] = mutable.HashSet()

            val groupOverviews = adminClient.listAllConsumerGroupsFlattened().filter(c => c.groupId != kafkaOffsetMonitorGroup)

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
                    committedOffsetMap.put(
                      new GroupTopicPartition(groupId, topicPartition),
                      new OffsetAndMetadata(new OffsetMetadata(offset = offset), commitTimestamp = 0, expireTimestamp = 0)
                    )
                    logEndOffsetsMap.put(topicPartition, 0)

                    // adding these fake consumers in order to show empty entries in any case
                    currentActiveTopicPartitions += TopicAndPartition(topicPartition.topic(), topicPartition.partition())
                    currentTopicAndGroups += KafkaTopicGroup(topicPartition.topic(), groupId)
                  }
                }
                currentClients += KafkaClientGroup(groupId, "", "", offsets.keys.toSet)
              }
            })

            groups = (for (x <- groupOverviews) yield x.groupId) (collection.breakOut).toSet
            activeTopicPartitions = currentActiveTopicPartitions.toSet
            clients = currentClients.toSet
            topicAndGroups = currentTopicAndGroups.toSet
          }
          catch {
            case e: Throwable =>
              error("Kafka AdminClient polling aborted due to an unexpected exception", e)
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

        Await.result(f, duration.pairIntToDuration(awaitForResults, duration.MILLISECONDS))

        Thread.sleep(sleepDurationMillis)
      } catch {
        case tex: java.util.concurrent.TimeoutException => {
          warn("The AdminClient timed out, so it will be closed and restarted", tex)
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
    }
  }

  def startLogEndOffsetGetter(args: OffsetGetterArgs) = {

    val group: String = "kafka-monitor-log-end-offset-getter"
    val sleepOnDataRetrieval: Int = 10000
    val sleepOnError: Int = 30000
    var logEndOffsetGetter: KafkaConsumer[Array[Byte], Array[Byte]] = null

    while (true) {

      try {
        if (logEndOffsetGetter == null) {
          logEndOffsetGetter = createNewKafkaConsumer(args, group)
        }

        // Get topic-partitions
        topicPartitionsMap = JavaConversions.mapAsScalaMap(logEndOffsetGetter.listTopics()).toMap
        val distinctPartitionInfo: Seq[PartitionInfo] = (topicPartitionsMap.values).flatten(listPartitionInfo => JavaConversions.asScalaBuffer(listPartitionInfo)).toSeq

        // Iterate over each distinct PartitionInfo
        distinctPartitionInfo.foreach(partitionInfo => {
          // Get the LogEndOffset for the TopicPartition
          val topicPartition: TopicPartition = new TopicPartition(partitionInfo.topic, partitionInfo.partition)
          logEndOffsetGetter.assign(Arrays.asList(topicPartition))
          logEndOffsetGetter.seekToEnd(Arrays.asList(topicPartition))
          val logEndOffset: Long = logEndOffsetGetter.position(topicPartition)

          // Update the TopicPartition map with the current LogEndOffset if it exists, else add a new entry to the map
          if (logEndOffsetsMap.contains(topicPartition)) {
            logEndOffsetsMap.update(topicPartition, logEndOffset)
          }
          else {
            logEndOffsetsMap += (topicPartition -> logEndOffset)
          }
        })

        Thread.sleep(sleepOnDataRetrieval)
      }

      catch {
        case e: Throwable => {
          error("The Kafka Client reading topic/partition LogEndOffsets has thrown an unhandled exception. Will attempt to reconnect", e)

          if (logEndOffsetGetter != null) {
            try {
              logEndOffsetGetter.close()
            } catch {
              case ex: Throwable => ()
            }
            logEndOffsetGetter = null
          }
        }
      }
    }
  }
}

case class KafkaTopicGroup(topic: String, group: String)

case class KafkaClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicPartition])
