package com.de.recs.core

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.udf


object ConvertFactorsToCsvJob extends BaseSparkJob {

  val USER_FACTORS_INPUT = "userFactorsInput"
  val USER_FACTORS_OUTPUT = "userFactorsOutput"
  val PRODUCT_FACTORS_INPUT = "productFactorsInput"
  val PRODUCT_FACTORS_OUTPUT = "productFactorsOutput"
  val PRODUCT_SIMILARITY_INPUT = "productSimilarityInput"
  val PRODUCT_SIMILARITY_OUTPUT = "productSimilarityOutput"

  val MALE = "male"
  val FEMALE = "female"
  val TAB_SEPARATOR = "\t"

  override def run(spark: SparkSession, args: Map[String, String]) {

    if (args.keySet.contains(USER_FACTORS_OUTPUT)) {
      convertFactors(spark, args(USER_FACTORS_INPUT), args(USER_FACTORS_OUTPUT), 32)
    }
    if (args.keySet.contains(PRODUCT_FACTORS_OUTPUT)) {
      convertProductFactors(spark, args(PRODUCT_FACTORS_INPUT), args(PRODUCT_FACTORS_OUTPUT), 100)
    }
    if (args.keySet.contains(PRODUCT_SIMILARITY_OUTPUT)) {
      convert(spark, args(PRODUCT_SIMILARITY_INPUT), args(PRODUCT_SIMILARITY_OUTPUT), 64)
    }
  }

  private def convertProductFactors(spark: SparkSession,
                             inputPath: String,
                             outputPath: String,
                             numPartitions: Int) {
    logger.info("convertFactors:+ inputPath=%s, outputPath=%s", Array(inputPath, outputPath): _*)
    val df = spark.read.parquet(makePath(inputPath, MALE), makePath(inputPath, FEMALE))
    val stringify = udf((id: Int, xs: Seq[Double]) => id.toString + "," + xs.mkString(","))

    val converted = df.select(stringify(df("id"), df("factors")))

    // a bit of a hack. For backwarf compatibility we need to use a tab to seprate the id and ALSO the factor.
    // using tab below will present quoting the string
    saveText(converted, outputPath, "\\t", numPartitions)
    logger.info("convertFactors:-")
  }

  private def convertFactors(spark: SparkSession,
                             inputPath: String,
                             outputPath: String,
                             numPartitions: Int,
                             delimiter: String = TAB_SEPARATOR) {
    logger.info("convertFactors:+ inputPath=%s, outputPath=%s", Array(inputPath, outputPath): _*)
    val df = spark.read.parquet(makePath(inputPath, MALE), makePath(inputPath, FEMALE))
    val stringify = udf((xs: Seq[Double]) => xs.mkString(","))

    val converted = df.select(df("id"), stringify(df("factors")))

    saveText(converted, outputPath, delimiter, numPartitions)
    logger.info("convertFactors:-")
  }


  private def convert(spark: SparkSession,
                      inputPath: String,
                      outputPath: String,
                      numPartitions: Int,
                      delimiter: String = TAB_SEPARATOR) {
    logger.info("convert:+ inputPath=%s, outputPath=%s", Array(inputPath, outputPath): _*)
    val df = spark.read.parquet(makePath(inputPath, MALE), makePath(inputPath, FEMALE))
    saveText(df, outputPath, delimiter, numPartitions)
    logger.info("convert:-")
  }

  private def makePath(base: String, more: String*): String = {
    more.foldLeft(base)((acc, s) => {
      val start = if (acc.endsWith("/") || s.startsWith("/")) acc else acc + "/"
      val end = if (s.startsWith("/")) s.substring(1) else s
      start + end
    })
  }

  private def saveText[T](dataFrame: Dataset[T], path: String, delimiter: String, numPartitions: Int) {
    dataFrame.coalesce(numPartitions).write
      .option("delimiter", delimiter)
      .option("nullValue", "\\N")
      .option("headers", "false")
      .csv(path)
  }
}
