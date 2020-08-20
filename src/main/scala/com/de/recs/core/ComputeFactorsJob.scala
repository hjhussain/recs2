package com.de.recs.core

import com.asos.datascience.recs.common.SimilarityScore
import com.de.recs.common.{ItemFactors, SimilarityScore}
import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

import scala.math.max
import scala.util.Try

case class ProductFeatures(stockLevel: Int)

object ComputeFactorsJob extends BaseSparkJob {


  override def run(spark: SparkSession, args: Map[String, String]) {
    val spark = SparkSession.builder().appName("ComputeFactorsADF").getOrCreate()

    val todaysDate = DateTime.parse(args("todaysDate"))
    val customerPath = args("customerPath")
    val productPath = args("productPath")
    val isRecommendedPath = args("isRecommendedPath")
    val signalMonths = args("signalMonths").toInt
    val signalsPath = args("signalsPath")

    // available products
    val products: collection.Map[Int, ProductFeatures] = collectProducts(spark.sparkContext, productPath, todaysDate)
    val productMapBB: Broadcast[collection.Map[Int, ProductFeatures]] = spark.sparkContext.broadcast(products)
    // customers with
    val customerTraining = collectCustomers(spark.sparkContext, customerPath = customerPath)
    val customerTrainingBB: Broadcast[Seq[Int]] = spark.sparkContext.broadcast(customerTraining)

    val validProducts = spark.sparkContext.textFile(isRecommendedPath)
      .map(line => Try {
        val tokens = line.split("\t")
        (tokens(0).toInt, tokens(1).toInt)
      })
      .filter(_.isSuccess).map(_.get)
      .filter(_._2 == 1)
      .map(_._1).collect.toSet
    val validProductsBB: Broadcast[Set[Int]] = spark.sparkContext.broadcast(validProducts)


    // load signals once
    val signalsRDD = spark.sparkContext.textFile(getTrainingString(prefix = signalsPath, today = todaysDate,
      signalMonths))
    signalsRDD.persist(StorageLevel.MEMORY_ONLY)

    generateOutput(spark, signalsRDD, customerTrainingBB, productMapBB, validProductsBB, args)

  }

  def generateOutput(spark: SparkSession,
                     signalsRDD: RDD[String],
                     customerGenderTrainingBB: Broadcast[collection.Seq[Int]],
                     productMapBB: Broadcast[collection.Map[Int, ProductFeatures]],
                     validProductsBB: Broadcast[Set[Int]],
                     params: Map[String, String]) {
    import spark.implicits._
    val prodSimPath = params("prodSimPath")
    val prodFactorsPath = params("prodFactorsPath")
    val userFactorsPath = params("userFactorsPath")
    val modelBasePath = params("modelPath")
    val useExistingModel = params("useExistingModel").toBoolean
    val customerFactorsPartitions = params.getOrElse("customerFactorsPartitions", "64").toInt
    val maxDaysOnSite = params("maxDaysOnSite").toInt
    val numRecs = params.getOrElse("numRecs", "100").toInt

    //  train the model
    if (!useExistingModel) {
      val ratingsRDD: RDD[Rating] = signalsToRatings(signalsRDD,
        customerGenderTrainingBB, productMapBB)

      val model = ALS.trainImplicit(ratingsRDD,
        params("rank").toInt,
        params("numIterations").toInt,
        params("lambda").toDouble,
        params("alpha").toDouble)

      model.save(spark.sparkContext, modelBasePath)
    }
    val loadedModel = MatrixFactorizationModel.load(spark.sparkContext, modelBasePath)

    val recs = recommendSimilarProducts(loadedModel,
      productMapBB,
      validProductsBB,
      numRecs,
      maxDaysOnSite)
    val similarities = recs.map(item => SimilarityScore(item._1, item._2.product, item._2.rating)).coalesce(32).toDS

    similarities.write.parquet(prodSimPath)

    // save product factors
    loadedModel.productFeatures
      .filter(item => productMapBB.value(item._1).isExcluded != 1 &&
        productMapBB.value(item._1).daysOnSite <= maxDaysOnSite &&
        productMapBB.value(item._1).stockLevel > 0
      )
      .map(item => ItemFactors(item._1, item._2)).toDS().write.parquet(prodFactorsPath )

    // save user factors
    loadedModel.userFeatures.coalesce(customerFactorsPartitions)
      .map(item => ItemFactors(item._1, item._2)).toDS().write.parquet(userFactorsPath)
  }

  private def signalsToRatings(signalsRDD: RDD[String],
                               customerTrainingBB: Broadcast[collection.Seq[Int]],
                               productMapBB: Broadcast[collection.Map[Int, ProductFeatures]]) = {
    signalsRDD.map(parseLine)
      .filter(item =>
        item._1._1 != -1 &&
          customerTrainingBB.value.isDefinedAt(item._1._1) &&
          productMapBB.value.isDefinedAt(item._1._2)
      )
      .reduceByKey((r1, r2) => max(r1, r2))
      .map(item => Rating(item._1._1, item._1._2, item._2))
      .persist(StorageLevel.MEMORY_ONLY)
  }

  private def recommendSimilarProducts(model: MatrixFactorizationModel,
                                       productMapBB: Broadcast[collection.Map[Int, ProductFeatures]],
                                       validProductsBB: Broadcast[Set[Int]],
                                       numRecs: Int,
                                       maxDaysOnSite: Int): RDD[(Int, Rating)] = {
    val productFeatures = model.productFeatures.coalesce(16).persist(StorageLevel.MEMORY_ONLY)
    val candidateFeatures = productFeatures
      .filter(item => validProductsBB.value.contains(item._1) &&
        productMapBB.value(item._1).isExcluded != 1 &&
        productMapBB.value(item._1).daysOnSite <= maxDaysOnSite &&
        productMapBB.value(item._1).stockLevel > 0)
    val modelSubset = new MatrixFactorizationModel(rank = model.rank, productFeatures, candidateFeatures)

    modelSubset.recommendProductsForUsers(numRecs)
      .flatMapValues(item => item).filter(item => item._2.product != item._1 && item._2.rating > 0.0)
  }

  def getTrainingString(prefix: String, today: DateTime, signalMonths: Int): String = {
    // val prefix = "wasb://signals@asosrecssignals.blob.core.windows.net/signals/*/"
    (0 to signalMonths).map(monthIdx => prefix + today.minusMonths(monthIdx).toString("'yy='yyyy'/mm='MM'/dd=*/'"))
      .mkString(",")
  }

  def getViewString(prefix: String, today: DateTime, viewMonths: Int): String = {
    // val prefix = "wasb://signals@asosrecssignals.blob.core.windows.net/productviews/*/productviews/"
    (0 to viewMonths).map(idx => prefix + today.minusMonths(idx).toString("'yy='YYYY'/mm='MM'/*/'")).mkString(",")
  }

  def getRating(sourceID: Int): Float = sourceID match {
    case 1 => 3.0f
    case 2 => 2.0f
    case 3 => 2.0f
    case 4 => 2.0f
    case 5 => 2.0f
    case 6 => 2.0f
    case 7 => 1.0f
    case _ => 0.0f
  }



  def parseLine(line: String): ((Int, Int), Float) = {
    val tokens: Array[String] = line.split('|')
    val customerID = NumberUtils.toInt(tokens(0), -1)
    val productID = NumberUtils.toInt(tokens(1), -1)
    val sourceID = NumberUtils.toInt(tokens(4), -1)
    val rating = getRating(sourceID)

    if (customerID == -1 || productID == -1 || sourceID == -1 || rating < 1 || !isValidDate(tokens(6))) {
      ((-1, -1), -1f)
    } else {
      ((customerID, productID), rating)
    }
  }


  def collectCustomers(sc: SparkContext, customerPath: String): collection.Seq[Int] = {
    sc.textFile(customerPath)
      .map(line => {
        val tokens = line.split('\t')
        if (tokens.length != 8) {
           -1
        } else {
          val customerID = NumberUtils.toInt(tokens(0), -1)
          customerID
        }
      })
      .filter(_ != -1)
      .collect()
  }

  def isValidDate(dateString: String): Boolean = {
    try {
      val checkDate = DateTime.parse(dateString)
    } catch {
      case ex: IllegalArgumentException => return false
    }
    true
  }

  def collectProducts(sc: SparkContext,
                      productPath: String,
                      todaysDate: DateTime): scala.collection.Map[Int, ProductFeatures] = {
    /*
    0  productId
    1  name
    2  brandName
    3  brandId
    4  category
    5  departmentId
    6  department
    7  divisionId
    8  division
    9  season
    11 statusId
    12 currentPrice
    13 dateModified
    14 stockLevel
    15 productType
    16 productTypeId
    17 highLevelProductType
    18 highLevelProductTypeId
    19 brandFamily
    20 isAsos
    21 isMaternity
    22 isPolarizing
    23 isExcluded
    24 isChristmas     */

    sc.textFile(productPath)
      .map(line => Try {
        val tokens = line.split('|')
        val productId = tokens(0).toInt


        if (productId == -1) {
          throw new IllegalArgumentException
        } else {
          (productId, ProductFeatures(stockLevel = stockLevel, price = currentPrice,
            highLevelProductTypeId = highLevelProductTypeId, isExcluded = isExcluded))
        }
      }).filter(_.isSuccess).map(_.get)
      .collectAsMap
  }
}
