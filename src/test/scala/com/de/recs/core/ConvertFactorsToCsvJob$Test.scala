package com.de.recs.core

import java.io.File

import com.de.recs.BaseSparkSuite
import org.apache.commons.io.FileUtils

class ConvertFactorsToCsvJob$Test extends BaseSparkSuite {

  val WORKING_DIR = "target/tests/savefactorscsv"


  override def beforeAll() {
    super.beforeAll()

    FileUtils.deleteDirectory(new File(WORKING_DIR))
  }

  test("test Csv conversion is accurate") {
    // run matrix factorization and generate data
    val computeFactorArgs = computeFactorsArgs()
    ComputeFactorsJob.run(sparkSession, computeFactorArgs)

    val arguments = Map("userFactorsInput" -> computeFactorArgs("userFactorsPath"),
      "productFactorsInput" -> computeFactorArgs("prodFactorsPath"),
      "productSimilarityInput" -> computeFactorArgs("prodSimPath"),
      "userFactorsOutput" -> (WORKING_DIR + "/userFactorsOutputCsv"),
      "productFactorsOutput" -> (WORKING_DIR + "/productFactorsOutputCsv"),
      "productSimilarityOutput" -> (WORKING_DIR + "/productSimilarityOutputCsv"))

    ConvertFactorsToCsvJob.run(sparkSession, arguments)

    val userFactors = sparkSession.read.option("delimiter", "\\t").csv(arguments("userFactorsOutput"))
    val productFactors = sparkSession.read.option("delimiter", ",").csv(arguments("productFactorsOutput"))
    val similarityFactors = sparkSession.read.option("delimiter", "\\t").csv(arguments("productSimilarityOutput"))
    userFactors.show(false)
    productFactors.show(false)
    similarityFactors.show(false)

    similarityFactors.columns.length should be(3)
    userFactors.columns.length should be(2)
    productFactors.columns.length should be(11)

    userFactors.count should not be 0
    productFactors should not be 0
    similarityFactors should not be 0
  }


  private def computeFactorsArgs(): Map[String, String] = {
    Map(
      "rank" -> "10",
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

      "customerPath" -> "src/test/resources/core/integration/scenario1/customers",
      "productPath" -> "src/test/resources/core/integration/scenario1/products",
      "isRecommendedPath" -> "src/test/resources/core/integration/scenario1/isRecommended",
      "signalsPath" -> "src/test/resources/core/integration/scenario1/signals/*/",
      "productViewsPath" -> "src/test/resources/core/integration/scenario1/productviews/*/productviews/",
      "prodSimPath" -> (WORKING_DIR + "/scenario1/prodSim"),
      "prodFactorsPath" -> (WORKING_DIR + "/scenario1/prodFactors"),
      "userFactorsPath" -> (WORKING_DIR + "/scenario1/userFactors"),
      "modelPath" -> (WORKING_DIR + "/scenario1/model"))
  }

}
