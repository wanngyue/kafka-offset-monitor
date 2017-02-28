package com.quantifind.kafka.offsetapp

import com.quantifind.kafka.OffsetGetter.KafkaOffsetInfo

trait OffsetInfoReporter {
  def report(info: IndexedSeq[KafkaOffsetInfo])
  def cleanupOldData() = {}
}
