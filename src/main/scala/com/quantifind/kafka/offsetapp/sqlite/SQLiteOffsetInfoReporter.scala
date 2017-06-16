package com.quantifind.kafka.offsetapp.sqlite

import com.quantifind.kafka.OffsetGetter.KafkaOffsetInfo
import com.quantifind.kafka.offsetapp.{OWArgs, OffsetDB, OffsetInfoReporter}

class SQLiteOffsetInfoReporter(db: OffsetDB, args: OWArgs) extends OffsetInfoReporter {

  override def report(info: IndexedSeq[KafkaOffsetInfo]): Unit = {
    db.insertAll(info)
  }

  override def cleanupOldData {
    db.emptyOld(System.currentTimeMillis - args.retain.toMillis)
  }

}
