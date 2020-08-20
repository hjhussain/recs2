package com.de.recs.core

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

trait BaseSparkJob {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)


  def run(spark: SparkSession, args: Map[String, String])


  final def main(cmdArgs: Array[String]) {
    val args = BaseSparkJob.getNamedArgs(cmdArgs)
    val spark = BaseSparkJob.sparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    val jobName = getClass.getSimpleName.replace("$", "")

    try {
      logger.info("Started-{}", jobName)

      run(spark, args)

      logger.info("Completed Successfully-{}", jobName)

    } catch {
      case ex: Throwable =>
        logger.error(s"Job Failed-$jobName", ex)
        throw ex
    } finally {
      Try(spark.stop())
    }
    logger.info("{} is complete", jobName)
  }
}

object BaseSparkJob {

  def sparkSession(master: String = "local[*]"): SparkSession = {
    SparkSession.builder()
      .appName(getClass.getName)
      .master(master)
      .getOrCreate()
  }

  def getNamedArgs(args: Array[String]): Map[String, String] = {
    args.filter(line => line.contains("="))
      .map(_.split("="))
      .map(parts => (parts(0), parts(1)))
      .toMap
  }
}
