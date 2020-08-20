package com.de.recs.core

import java.io.{ByteArrayInputStream, DataInputStream, File}

import com.de.recs.BaseSparkSuite
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Integration Test for the recommendation algorithm
  */
class RecsIntTest extends BaseSparkSuite {

  private val WORKING_DIR = "target/tests/output/core/integration"
  val iisSchema = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("data", StringType, nullable = false)
  ))
  val prodFactorsSchema = StructType(Array(
    StructField("id", IntegerType, nullable = false),
    StructField("gender", StringType, nullable = false),
    StructField("data", StringType, nullable = false)
  ))
  val custFactorsSchema = StructType(Array(
    StructField("gnuid", StringType, nullable = false),
    StructField("data", StringType, nullable = false)
  ))


  override def beforeAll() {
    super.beforeAll()
    FileUtils.deleteDirectory(new File(WORKING_DIR).getParentFile)
  }

  test("test scenario 1 - verifying the base64 contracts") {

    runJobs(computeFactorsArgs(1), saveFactorsArgs(1))

    // load the output
    val userFactorsDF = sparkSession.read.option("delimiter", "\t").schema(custFactorsSchema)
      .csv(makeOutputPath(1, "final/userFactors"))
    val productFactorsDF = sparkSession.read.option("delimiter", "\t").schema(prodFactorsSchema)
      .csv(makeOutputPath(1, "final/productFactors"))
    val prodSimilarityDF = sparkSession.read.option("delimiter", "\t").schema(iisSchema)
      .csv(makeOutputPath(1, "final/productSimilarity"))

    userFactorsDF.show()
    productFactorsDF.show()
    prodSimilarityDF.show()

    // verify the counts of product factors
    productFactorsDF.count() > 0 should be(true)
    productFactorsDF.groupBy("gender").count().where("gender='F'").first().getAs[Long]("count") > 0 should be(true)
    productFactorsDF.groupBy("gender").count().where("gender='M'").first().getAs[Long]("count") > 0 should be(true)

    // verify the counts of customer factors
    userFactorsDF.count() > 0 should be(true)

    // verify the counts of similar scores
    prodSimilarityDF.count() > 0 should be(true)

    // verify the product factors
    val firstProductFactor = productFactorsDF.first()
    val prodFactor = firstProductFactor.getAs[String]("data")
    val prodFactorArray = decodeBase64(prodFactor)
    prodFactorArray.length should be(800)
    val actualProdFactors = getFloatArray(prodFactorArray)
    val dsOriginalProdFactors = sparkSession.read.parquet(makePathGender(1, "prodFactors", "female"),
      makePathGender(1, "prodFactors", "male"))
    val expectedProdFactors = dsOriginalProdFactors.where("id=" + firstProductFactor.getAs[Int]("id"))
      .first().getAs[mutable.WrappedArray[Double]]("factors").toArray
    actualProdFactors should contain theSameElementsAs expectedProdFactors

    val customerDF = DataLoader.customerLoader.loadTextData(sparkSession, "\t", makeInputPath(1, "customers"))

    // verify the customer factors
    val firstCustomerFactor = userFactorsDF.join(customerDF, userFactorsDF("gnuid") === customerDF("guid"))
      .select("gnuid", "customerId", "data").first()
    val custFactor = firstCustomerFactor.getAs[String]("data")
    val custFactorArray = decodeBase64(custFactor)
    custFactorArray.length should be(800)
    firstCustomerFactor.getAs[String]("gnuid").length should be(32)
    val actualCustFactors = getFloatArray(custFactorArray)
    val dsOriginalCustFactors = sparkSession.read.parquet(makePathGender(1, "userFactors", "female"),
      makePathGender(1, "userFactors", "/male"))
    val expectedCustFactors = dsOriginalCustFactors.where("id=" + firstCustomerFactor.getAs[Int]("customerId"))
      .first().getAs[mutable.WrappedArray[Double]]("factors").toArray
    actualCustFactors should contain theSameElementsAs expectedCustFactors

    // verify item similarities
    val firstSimilarScore = prodSimilarityDF.first()
    val prodScores = firstSimilarScore.getAs[String]("data")
    val prodScoresArray = decodeBase64(prodScores)
    prodScoresArray.length > 0 should be(true)
    val actualSimilarScores = getProdSimilarScores(prodScoresArray)
    val dsOriginalScores = sparkSession.read.parquet(makePathGender(1, "prodSim", "female"),
      makePathGender(1, "prodSim", "male"))
    import dsOriginalScores.sparkSession.implicits._
    val expectedScores = dsOriginalScores.where("id=" + firstSimilarScore.getAs[Int]("id"))
      .map(r => (r.getAs[Int]("similarId"), r.getAs[Double]("score").toFloat)).collect()
    actualSimilarScores should contain theSameElementsAs expectedScores

  }

  test("test scenario 2 - non recommended products should not be included in recs") {
    runJobs(computeFactorsArgs(2), saveFactorsArgs(2))

    // load the output
    val productFactorsDF = sparkSession.read.option("delimiter", "\t").schema(prodFactorsSchema)
      .csv(makeOutputPath(2, "final/productFactors"))
    val prodSimilarityDF = sparkSession.read.option("delimiter", "\t").schema(iisSchema)
      .csv(makeOutputPath(2, "final/productSimilarity"))

    // verify the product ids
    val productFactorIds = productFactorsDF.select("id").collect().map(r => r.getAs[Int]("id"))
    val simTargetProductIds = prodSimilarityDF.select("id").collect().map(r => r.getAs[Int]("id"))
    val simCandidateProductIds = prodSimilarityDF
      .select("data").collect().map(_.getAs[String]("data"))
      .map(data => getProdSimilarScores(decodeBase64(data)))
      .flatMap(_.map(_._1))
      .distinct

    // list of products that should not be recommended
    val ids = Set(12, 44, 48, 76, 96)

    productFactorIds.count(ids.contains) should be(0)
    simTargetProductIds.count(ids.contains) should be(ids.size)
    simCandidateProductIds.count(ids.contains) should be(0)

  }

  test("test scenario 3 - Products older than 545 days should not be included in output") {

    runJobs(computeFactorsArgs(3), saveFactorsArgs(3))
    // load the output
    val userFactorsDF = sparkSession.read.option("delimiter", "\t").schema(custFactorsSchema)
      .csv(makeOutputPath(3, "final/userFactors"))
    val productFactorsDF = sparkSession.read.option("delimiter", "\t").schema(prodFactorsSchema)
      .csv(makeOutputPath(3, "final/productFactors"))
    val prodSimilarityDF = sparkSession.read.option("delimiter", "\t").schema(iisSchema)
      .csv(makeOutputPath(3, "final/productSimilarity"))

    userFactorsDF.show()
    productFactorsDF.orderBy("id").show()
    prodSimilarityDF.orderBy("id").show()

    val productFactorIds = productFactorsDF.select("id").collect().map(r => r.getAs[Int]("id"))
    val simTargetProductIds = prodSimilarityDF.select("id").collect().map(r => r.getAs[Int]("id"))
    val simCandidateProductIds = prodSimilarityDF
      .select("data").collect().map(_.getAs[String]("data"))
      .map(data => getProdSimilarScores(decodeBase64(data)))
      .flatMap(_.map(_._1))
      .distinct

    // List of ids in products.csv that are older than 545 days
    val ids = List(5, 48, 55, 90)

    productFactorIds.count(ids.contains) should be(0)
    simTargetProductIds.count(ids.contains) should be(ids.size)
    simCandidateProductIds.count(ids.contains) should be(0)
  }

  private def runJobs(computeArgs: Map[String, String],
                      saveArgs: Map[String, String]) {
    // run matrix factorization and generate data
    ComputeFactorsJob.run(sparkSession, computeArgs)
    // save output in base64 contracts
    ConvertFactorsToBase64Job.run(sparkSession, saveArgs)
  }

  def decodeBase64(s: String): Array[Byte] = {
    java.util.Base64.getDecoder.decode(s)
  }


  def getFloatArray(data: Array[Byte]): Array[Float] = {
    val dataIn = new DataInputStream(new ByteArrayInputStream(data))
    val result = new Array[Float](200)
    for (i <- 0 to 199) {
      result(i) = dataIn.readFloat()
    }
    result
  }

  def getProdSimilarScores(data: Array[Byte]): Array[(Int, Float)] = {
    val dataIn = new DataInputStream(new ByteArrayInputStream(data))
    val result = new Array[(Int, Float)](data.length / 8)
    for (i <- 0 until data.length / 8) {
      val tuple = (dataIn.readInt(), dataIn.readFloat())
      result(i) = tuple
    }
    result
  }

  def makeInputPath(scenario: Int, dataset: String): String = {
    s"src/test/resources/core/integration/scenario$scenario/$dataset"
  }

  def makeOutputPath(scenario: Int, dataset: String): String = s"$WORKING_DIR/scenario$scenario/$dataset"

  def makePathGender(scenario: Int, dataset: String, gender: String): String = {
    makeOutputPath(scenario, dataset) + "/" + gender
  }

  def computeFactorsArgs(scenario: Int): Map[String, String] = {
    Map(
      "rank" -> "200",
      "numIterations" -> "1",
      "alpha" -> "400",
      "lambda" -> "0.1",
      "numRecs" -> "100",
      "productAgeTh" -> "560",
      "todaysDate" -> "2017-08-06",
      "minStockGifts" -> "10",
      "useExistingModel" -> "false",
      "viewMonths" -> "1",
      "signalMonths" -> "1",
      "maxDaysOnSite" -> "545",

      "customerPath" -> makeInputPath(scenario, "customers"),
      "productPath" -> makeInputPath(scenario, "products"),
      "isRecommendedPath" -> makeInputPath(scenario, "isRecommended"),
      "signalsPath" -> makeInputPath(scenario, "signals/*/"),
      "productViewsPath" -> makeInputPath(scenario, "productviews/*/productviews/"),

      "prodSimPath" -> makeOutputPath(scenario, "prodSim"),
      "prodFactorsPath" -> makeOutputPath(scenario, "prodFactors"),
      "userFactorsPath" -> makeOutputPath(scenario, "userFactors"),
      "modelPath" -> makeOutputPath(scenario, "model")
    )
  }

  def saveFactorsArgs(scenario: Int): Map[String, String] = {
    Map(
      "customersInput" -> makeInputPath(scenario, "customers"),
      "userFactorsInput" -> makeOutputPath(scenario, "userFactors"),
      "productFactorsInput" -> makeOutputPath(scenario, "prodFactors"),
      "productSimilarityInput" -> makeOutputPath(scenario, "prodSim"),

      "userFactorsOutput" -> makeOutputPath(scenario, "final/userFactors"),
      "productFactorsOutput" -> makeOutputPath(scenario, "final/productFactors"),
      "productSimilarityOutput" -> makeOutputPath(scenario, "final/productSimilarity")
    )
  }
}
