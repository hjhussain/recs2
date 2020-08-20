package com.de.recs.dataprep

import com.asos.datascience.framework.core.BaseJobWithMonitoring
import com.asos.datascience.framework.core.types.{Customer, GenericSignal}
import com.asos.datascience.framework.data.DataLoader
import com.asos.datascience.framework.data.common.JobUtils
import com.de.recs.core.BaseSparkJob
import data.DataLoader
import org.apache.spark.sql.{Dataset, SparkSession}

object FilterableSignalsJob extends BaseSparkJob {

  case class SignalWithGuid(customerId: Int,
                            guid: String,
                            productId: Int,
                            sourceId: Int,
                            signalDate: Long)

  case class Signal(guid: String,
                    productId: Int,
                    sourceId: Int,
                    signalDate: String)

  val SIGNALS_INPUT_PATH = "signalsPath"
  val CUSTOMERS_INPUT_PATH = "customersPath"
  val CVS_OUTPUT_PATH = "csvOutputPath"

  override def run(spark: SparkSession, args: Map[String, String]) {

    val genericSignals = DataLoader.signalsLoader.loadParquet(spark, args(SIGNALS_INPUT_PATH))
    val customers = DataLoader.customerLoader.loadParquet(spark, args(CUSTOMERS_INPUT_PATH))

    val signals = toSignalsWithGuid(genericSignals, customers)

    val prioritySignals = prioritiseSignals(signals)
    val recsSignals = mapToRequiredOutput(prioritySignals)

    JobUtils.saveText(recsSignals, args(CVS_OUTPUT_PATH), "\t", 1)
  }

  def toSignalsWithGuid(signals: Dataset[GenericSignal],
                        customers: Dataset[Customer]): Dataset[SignalWithGuid] = {
    import signals.sparkSession.implicits._

    val df = customers.join(signals, Seq("customerId"))

    df.map(r => SignalWithGuid(r.getAs[Int]("customerId"),
      r.getAs[String]("guid"),
      r.getAs[Int]("productId"),
      r.getAs[Int]("sourceId"),
      r.getAs[Long]("signalDate")))
  }

  def prioritiseSignals(signals: Dataset[SignalWithGuid]): Dataset[SignalWithGuid] = {
    import signals.sparkSession.implicits._
    signals.groupByKey(s => (s.guid, s.productId))
      .reduceGroups(lowestSourceIdThenLatestModified _)
      .map(_._2)
  }

  def mapToRequiredOutput(signals: Dataset[SignalWithGuid]): Dataset[Signal] = {
    import signals.sparkSession.implicits._

    signals.map(s => Signal(s.guid,
      s.productId,
      s.sourceId,
      JobUtils.timestampToStr(s.signalDate)))
  }

  private def lowestSourceIdThenLatestModified(s1: SignalWithGuid, s2: SignalWithGuid): SignalWithGuid = {
    if (s1.sourceId < s2.sourceId || (s1.sourceId == s2.sourceId && s1.signalDate > s2.signalDate)) {
      s1
    } else {
      s2
    }
  }

}
