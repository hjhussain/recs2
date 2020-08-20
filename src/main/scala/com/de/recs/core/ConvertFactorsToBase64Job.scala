package com.de.recs.core

import java.nio.{ByteBuffer, ByteOrder}

import com.asos.datascience.recs.common.SimilarityScore
import com.de.recs.common.{ItemFactors, SimilarityScore}
import data.customer.CustomerLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable


object ConvertFactorsToBase64Job extends BaseSparkJob {

  val USER_FACTORS_INPUT = "userFactorsInput"
  val CUSTOMERS_INPUT = "customersInput"
  val USER_FACTORS_OUTPUT = "userFactorsOutput"
  val PRODUCT_FACTORS_INPUT = "productFactorsInput"
  val PRODUCT_FACTORS_OUTPUT = "productFactorsOutput"
  val PRODUCT_SIMILARITY_INPUT = "productSimilarityInput"
  val PRODUCT_SIMILARITY_OUTPUT = "productSimilarityOutput"

  val MALE = "male"
  val FEMALE = "female"
  val TAB_SEPARATOR = "\t"

  override def run(spark: SparkSession, args: Map[String, String]) {

    val customersDf = CustomerLoader.loadTextData(spark, TAB_SEPARATOR, args(CUSTOMERS_INPUT)).toDF()
    processCustomerFactors(spark, customersDf, args(USER_FACTORS_INPUT), args(USER_FACTORS_OUTPUT), 64)
    combineProductFactors(spark, args(PRODUCT_FACTORS_INPUT), args(PRODUCT_FACTORS_OUTPUT), 100)
    processSimilarity(spark, args(PRODUCT_SIMILARITY_INPUT), args(PRODUCT_SIMILARITY_OUTPUT), 64)

  }

  private def processCustomerFactors(spark: SparkSession,
                                     customersDf: DataFrame,
                                     inputPath: String,
                                     outputPath: String,
                                     numPartitions: Int,
                                     delimiter: String = TAB_SEPARATOR) {
    import spark.implicits._

    val factors = spark.read.parquet(makePath(inputPath, MALE), makePath(inputPath, FEMALE)).as[ItemFactors]

    val customers = customersDf.select(customersDf("customerId").as("id"), customersDf("guid").as("gnuid"))

    val customersAndFactorsDf = factors.join(customers, "id")

    val converted = customersAndFactorsDf.select("gnuid", "factors")
      .map(r => {
        val id = r(0).toString
        val factors = encodeToBase64String(r.getAs[mutable.WrappedArray[Double]](1).toArray)
        List(id, factors).mkString(delimiter)
      })

    converted.coalesce(numPartitions).write.csv(outputPath)
  }

  private def processProductFactors(spark: SparkSession,
                                    inputPath: String,
                                    gender: String,
                                    delimiter: String = TAB_SEPARATOR): Dataset[String] = {
    import spark.implicits._
    val genderChar = gender.charAt(0).toUpper
    val df = spark.read.parquet(makePath(inputPath, gender)).as[ItemFactors]
    val converted = df.map(r => {
      val id = r.id.toString
      val factors = encodeToBase64String(r.factors)
      List(id, genderChar, factors).mkString(delimiter)
    })
    converted
  }

  private def combineProductFactors(spark: SparkSession,
                                    inputPath: String,
                                    outputPath: String,
                                    numPartitions: Int,
                                    delimiter: String = TAB_SEPARATOR) {
    val maleFactors = processProductFactors(spark, inputPath, MALE, delimiter)
    val femaleFactors = processProductFactors(spark, inputPath, FEMALE, delimiter)
    val combinedFactors = maleFactors.union(femaleFactors)
    combinedFactors.coalesce(numPartitions).write.csv(outputPath)

  }

  private def processSimilarity(spark: SparkSession,
                                inputPath: String,
                                outputPath: String,
                                numPartitions: Int,
                                delimiter: String = TAB_SEPARATOR) {
    import spark.implicits._

    val df = spark.read.parquet(makePath(inputPath, "male"), makePath(inputPath, "female")).as[SimilarityScore]

    val converted = convertSimilaritiesToBase64(df, delimiter).toDS()

    converted.coalesce(numPartitions).write.csv(outputPath)
  }

  def convertSimilaritiesToBase64(df: Dataset[SimilarityScore], delimiter: String): RDD[String] = {
    val groupedByProductId = df.rdd.map(r => {
      val buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
      buf.putInt(r.similarId)
      buf.putFloat(4, r.score.toFloat)
      (r.id, buf.array())
    }).reduceByKey(_ ++ _)

    groupedByProductId.map(Function.tupled((id: Int, bytes: Array[Byte]) => {
      id.toString + delimiter + bytesToBase64(bytes)
    }))
  }

  def encodeToBase64String(a: Array[Double]): String = {
    val floatSize = 4
    val buf = ByteBuffer.allocate(a.length * floatSize).order(ByteOrder.BIG_ENDIAN)
    var pos: Int = 0

    a.foreach(v => {
      buf.putFloat(pos, v.toFloat)
      pos += floatSize
    })
    bytesToBase64(buf.array())
  }

  def bytesToBase64(buf: Array[Byte]): String = {
    java.util.Base64.getEncoder.encodeToString(buf)
  }

  def makePath(base: String, more: String*): String = {
    more.foldLeft(base)((acc, s) => {
      val start = if (acc.endsWith("/") || s.startsWith("/")) acc else acc + "/"
      val end = if (s.startsWith("/")) s.substring(1) else s
      start + end
    })
  }

  def decode(spark: SparkSession,
             df: DataFrame): Dataset[SimilarityScore] = {
    val result = df.rdd.flatMap(r => {
      val buffer = decodeBase64(r.getString(1))
      val values = decodesBytes(buffer)
      values.map(t => {
        Row(r.getInt(0), t._1, t._2)
      })
    })

    val schemaOut = StructType(Seq(
      StructField("id", IntegerType),
      StructField("similarId", IntegerType),
      StructField("score", FloatType)))

    import spark.implicits._
    spark.createDataFrame(result, schemaOut).as[SimilarityScore]
  }

  def decodeBase64(s: String): Array[Byte] = {
    java.util.Base64.getDecoder.decode(s)
  }

  def decodesBytes(b: Array[Byte]): Array[(Int, Float)] = {
    val bf = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN)
    Range(0, b.length, 8).map(i => {
      val id = bf.getInt(i)
      val score = bf.getFloat(i + 4)
      (id, score)
    }).toArray
  }

}
