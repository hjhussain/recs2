package com.de.recs.core

import java.io.File

import ConvertFactorsToBase64Job.TAB_SEPARATOR
import com.de.recs.BaseSparkSuite
import com.de.recs.common.SimilarityScore
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

class ConvertFactorsToBase64Job$Test extends BaseSparkSuite {

  val arguments = Map("userFactorsInput" -> "",
    "customersInput" -> "",
    "userFactorsOutput" -> "",
    "productFactorsInput" -> "",
    "productFactorsOutput" -> "",
    "productSimilarityInput" -> "",
    "productSimilarityOutput" -> "")

  val encodedSchema = StructType(Seq(
    StructField("productId", IntegerType),
    StructField("base64Str", StringType)))

  val WORKING_DIR = "target/tests/savefactors"


  override def beforeAll() {
    super.beforeAll()

    FileUtils.deleteDirectory(new File(WORKING_DIR))
  }

  test("test base64 conversion is accurate") {
    val outputPath = WORKING_DIR + "/product_similarities/converted.tsv"
    convertAndSaveSimilarities("src/test/resources/product_similarities.tsv", outputPath)

    val originalEncodedSimilarities = loadBase64(sparkSession, "src/test/resources/product_similarities_base64.tsv")

    val encodedSimilarities = loadBase64(sparkSession, outputPath)

    originalEncodedSimilarities.except(encodedSimilarities).count() should be(0)
  }

  private def convertAndSaveSimilarities(similaritiesPath: String, outputPath: String) {
    val similarities = loadSimilarities(sparkSession, similaritiesPath)

    val base64Rdd = ConvertFactorsToBase64Job.convertSimilaritiesToBase64(similarities, TAB_SEPARATOR)
    base64Rdd.coalesce(1).saveAsTextFile(outputPath)
  }

  def loadSimilarities(spark: SparkSession, path: String): Dataset[SimilarityScore] = {
    import spark.implicits._
    sparkSession.read
      .option("delimiter", TAB_SEPARATOR)
      .csv(path)
      .map(r => {
        SimilarityScore(r(0).toString.toInt, r(1).toString.toInt, r(2).toString.toDouble)
      })
      .as[SimilarityScore]
  }


  def loadBase64(spark: SparkSession, path: String): Dataset[SimilarityScore] = {
    val schema = StructType(Seq(
      StructField("productId", IntegerType),
      StructField("base64Str", StringType)))

    val df = spark.read
      .option("delimiter", TAB_SEPARATOR)
      .schema(schema)
      .csv(path)

    ConvertFactorsToBase64Job.decode(spark, df)
  }
}
