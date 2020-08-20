package com.de.recs

import java.nio.file.Paths

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


/**
  * Base class for unit tests. Extend your unit test from this class to get the in memory spark cluster support.
  *
  * @since 2.0
  */
class BaseSparkSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  System.setProperty("hadoop.home.dir", Paths.get("").toAbsolutePath.toString + "/src/test/resources/winutils/")
  var sparkSession: SparkSession = _
  var sqlContext: SQLContext = _
  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = newSparkSession
    sqlContext = sparkSession.sqlContext
    sc = sparkSession.sparkContext
    sparkSession.sqlContext.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll() {
    try {
      sparkSession.stop()
    } finally {
      super.afterAll()
    }
  }

  def newSparkSession: SparkSession = {
    LogManager.getRootLogger.setLevel(Level.ERROR)
    val path = Paths.get("").toAbsolutePath.toString + "/spark-warehouse"
    SparkSession.builder()
      .config("spark.sql.warehouse.dir", path) // fix for bug in windows
      .config("spark.ui.enabled", false)
      .config("spark.driver.host", "localhost")
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()
  }
}
