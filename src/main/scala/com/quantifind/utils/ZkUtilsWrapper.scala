package com.quantifind.utils

import kafka.cluster.Broker
import kafka.consumer.ConsumerThreadId
import kafka.utils.ZkUtils
import org.apache.zookeeper.data.Stat

import scala.collection.mutable

/**
  * This class is mainly to help us mock the ZkUtils class. It is really painful to get powermock to work with scalatest,
  * so we created this class with a little help from IntelliJ to auto-generate the delegation code
  */
class ZkUtilsWrapper(zkUtils: ZkUtils) {

  val ConsumersPath = ZkUtils.ConsumersPath

  val delegator: ZkUtils = zkUtils

  def readDataMaybeNull(path: String): (Option[String], Stat) = {
    delegator.readDataMaybeNull(path)
  }

  def readData(path: String): (String, Stat) = {
    delegator.readData(path)
  }

  def getPartitionsForTopics(topics: Seq[String]): mutable.Map[String, Seq[Int]] = {
    delegator.getPartitionsForTopics(topics)
  }

  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = {
    delegator.getLeaderForPartition(topic, partition)
  }

  def getConsumersPerTopic(group: String, excludeInternalTopics: Boolean): mutable.Map[String, List[ConsumerThreadId]] = {
    delegator.getConsumersPerTopic(group, excludeInternalTopics)
  }

  def getChildren(path: String): Seq[String] = {
    delegator.getChildren(path)
  }

  def getAllBrokersInCluster: Seq[Broker] = {
    delegator.getAllBrokersInCluster()
  }

  def close(): Unit = {
    delegator.close()
  }
}
