package com.quantifind.kafka.offsetapp

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.quantifind.kafka.{OffsetGetter, TopicDetails}
import com.quantifind.kafka.OffsetGetter.KafkaInfo
import com.quantifind.kafka.offsetapp.sqlite.SQLiteOffsetInfoReporter
import com.quantifind.sumac.validation.Required
import com.quantifind.utils.UnfilteredWebApp
import com.quantifind.utils.Utils.retry
import com.twitter.util.Time
import kafka.utils.Logging
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{CustomSerializer, JInt, NoTypeHints}
import unfiltered.filter.Plan
import unfiltered.request.{GET, Path, Seg}
import unfiltered.response.{JsonContent, Ok, ResponseString}

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.control.NonFatal

class OWArgs extends OffsetGetterArgs with UnfilteredWebApp.Arguments {

  @Required
  var retain: FiniteDuration = _

  @Required
  var refresh: FiniteDuration = _

  var dbName: String = "offsetapp"

  lazy val db = new OffsetDB(dbName)
}

/**
  * A webapp to track Kafka consumers and their offsets
  * User: pierre
  * Date: 1/23/14
  */
object OffsetGetterWeb extends UnfilteredWebApp[OWArgs] with Logging {

  implicit def funToRunnable(fun: () => Unit) = new Runnable() {
    def run() = fun()
  }

  def htmlRoot: String = "/offsetapp"

  val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(2)

  var dbReporter: SQLiteOffsetInfoReporter = null

  def reportOffsets(args: OWArgs) {
    val groups = getGroups(args)
    groups.foreach {
      g =>
        val inf = getInfo(g, args).offsets.toIndexedSeq
        if (dbReporter != null) {
          debug(s"reporting ${inf.size}")
          dbReporter.report(inf)
        }
    }
  }

  def schedule(args: OWArgs) {
    scheduler.scheduleAtFixedRate(() => {reportOffsets(args)}, 0, args.refresh.toMillis, TimeUnit.MILLISECONDS)
  }

  // -------------------------------------------------------------------------------------------------------------------
  // proxy operation to the real implementation of OffsetGetter
  def withOG[T](args: OWArgs)(f: OffsetGetter => T): T = {
    var og: OffsetGetter = null
    try {
      val offsetGetter: OffsetGetter = OffsetGetter.getInstance(args)
      f(offsetGetter)
    } finally {
      // ignored
    }
  }

  def getInfo(group: String, args: OWArgs): KafkaInfo = withOG(args) {
    _.getInfo(group)
  }

  def getGroups(args: OWArgs) = withOG(args) {
    _.getGroups
  }

  def getActiveTopics(args: OWArgs) = withOG(args) {
    _.getActiveTopics
  }

  def getTopics(args: OWArgs) = withOG(args) {
    _.getTopics
  }

  def getTopicDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicDetail(topic)
  }

  def getTopicAndConsumersDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicAndConsumersDetail(topic)
  }

  def getClusterViz(args: OWArgs) = withOG(args) {
    _.getClusterViz
  }

  // -------------------------------------------------------------------------------------------------------------------

  /**
    * Custom serializer Scala Time <-> epoch time
    */
  class TimeSerializer extends CustomSerializer[Time](
    format => ( {
      case JInt(s) => Time.fromMilliseconds(s.toLong)
    }, {
      case x: Time => JInt(x.inMilliseconds)
    })
  )

  override def setup(args: OWArgs): Plan = new Plan {
    args.db.maybeCreate()
    dbReporter = new SQLiteOffsetInfoReporter(args.db, args)

    // launch automatic Kafka state tracking
    schedule(args)

    // converting Scala datatypes to JSON format
    implicit val formats = Serialization.formats(NoTypeHints) + new TimeSerializer
    // define web application mapping
    def intent: Plan.Intent = {
      case GET(Path(Seg("group" :: Nil))) =>
        val groups: Seq[String] = getGroups(args)
        JsonContent ~> ResponseString(write(groups))

      case GET(Path(Seg("group" :: group :: Nil))) =>
        val info = getInfo(group, args)
        JsonContent ~> ResponseString(write(info)) ~> Ok

      case GET(Path(Seg("group" :: group :: topic :: Nil))) =>
        val offsets = args.db.offsetHistory(group, topic)
        JsonContent ~> ResponseString(write(offsets)) ~> Ok

      case GET(Path(Seg("topiclist" :: Nil))) =>
        val topics: Seq[String] = getTopics(args)
        JsonContent ~> ResponseString(write(topics))

      case GET(Path(Seg("clusterlist" :: Nil))) =>
        val node = getClusterViz(args)
        JsonContent ~> ResponseString(write(node))

      case GET(Path(Seg("topicdetails" :: topic :: Nil))) =>
        val topicDetails : TopicDetails = getTopicDetail(topic, args)
        JsonContent ~> ResponseString(write(topicDetails))

      case GET(Path(Seg("topic" :: topic :: "consumer" :: Nil))) =>
        val topicAndConsumersDetails = getTopicAndConsumersDetail(topic, args)
        JsonContent ~> ResponseString(write(topicAndConsumersDetails))

      case GET(Path(Seg("activetopics" :: Nil))) =>
        val node = getActiveTopics(args)
        JsonContent ~> ResponseString(write(node))
    }
  }

  override def afterStop() {
    scheduler.shutdown()
  }

}
