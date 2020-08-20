package com.de.recs.dataprep

import java.io.File

import FilterableSignalsJob._
import com.de.recs.BaseSparkSuite
import data.DataLoader
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class FilterableSignalsJobTest extends BaseSparkSuite {

  val WORKING_DIR = "target/tests/filterablesignals"
  implicit lazy val spark = sparkSession

  import spark.implicits._

  val arguments = Map(SIGNALS_INPUT_PATH -> s"$WORKING_DIR/customers/",
    CUSTOMERS_INPUT_PATH -> s"$WORKING_DIR/signals/",
    CVS_OUTPUT_PATH -> s"$WORKING_DIR/parquetResult")


  override def beforeAll() {
    super.beforeAll()
    FileUtils.deleteDirectory(new File(WORKING_DIR))
    val customersCsvPath = "src/test/resources/core/integration/scenario1/customers/customers.csv"
    val signalsCsvPath = "src/test/resources/core/integration/signals/signals_sample"
    val customers = DataLoader.customerLoader.loadTextData(sparkSession, "\t", customersCsvPath)
    val signals = DataLoader.signalsLoader.loadTextData(sparkSession, "|", signalsCsvPath)

    JobUtils.save(customers, arguments(CUSTOMERS_INPUT_PATH), 1)
    JobUtils.save(signals, arguments(SIGNALS_INPUT_PATH), 1)
  }

  test("testRun") {
    FilterableSignalsJob.run(sparkSession, arguments)

    val result = loadSavedOutput(arguments(CVS_OUTPUT_PATH))

    result.count should equal(11)

    val ds = result.map(s => (s.guid, s.productId))
    val distinctDs = ds.distinct()

    ds.except(distinctDs).count should equal(0)
  }

  test("Signals are prioritised by lowest sourceId first then modifiedDate second") {

    val prioritySignal = newSignal(customerId = 1, guid = "1", productId = 1, sourceId = 1, dateModified = 2)
    val allSignals = Seq(prioritySignal,
      newSignal(customerId = 1, guid = "1", productId = 1, sourceId = 1, dateModified = 1),
      newSignal(customerId = 1, guid = "1", productId = 1, sourceId = 2, dateModified = 3),
      newSignal(customerId = 1, guid = "1", productId = 1, sourceId = 3, dateModified = 4))

    val signalsDs = spark.createDataset(allSignals)

    val result = FilterableSignalsJob.prioritiseSignals(signalsDs)
    result.first() should equal(prioritySignal)
  }

  test("Signals are grouped based on guid") {

    val allSignals = Seq(newSignal(customerId = 1, guid = "1", productId = 1, sourceId = 1, dateModified = 3),
      newSignal(customerId = 2, guid = "1", productId = 1, sourceId = 2, dateModified = 3),
      newSignal(customerId = 3, guid = "2", productId = 1, sourceId = 3, dateModified = 4))

    val expected = allSignals.filterNot(_.customerId == 2)

    val signalsDs = spark.createDataset(allSignals)

    val result = FilterableSignalsJob.prioritiseSignals(signalsDs)
    result.collect().toSeq should equal(expected)
  }

  private def newSignal(customerId: Int,
                        guid: String,
                        productId: Int,
                        sourceId: Int,
                        dateModified: Long): SignalWithGuid = {
    SignalWithGuid(customerId, guid, productId, sourceId, dateModified)
  }

  private def loadSavedOutput(path: String) = {
    val schema = new StructType(Array(StructField("guid", StringType),
      StructField("productId", IntegerType),
      StructField("sourceId", IntegerType),
      StructField("signalDate", StringType)))

    spark.read.option("delimiter", "\t").schema(schema).csv(path).as[Signal]
  }

}
