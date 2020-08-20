package data.product


import com.de.recs.common.FieldNames.{FLD_DATE_MODIFIED, FLD_DATE_ON_SITE}
import data.common.Utils
import data.common.udfs.UDFS
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.Loader
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

/**
  * Implementation of Product Data Loader
  *
  * @since 2.0
  */
object ProductLoader extends Loader[Product] {


  /**
    * Loads Products data from one or more CSV locations into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Location of File
    * @param options   inputs options ie:delimiter,header
    * @return Dataset[Product]
    */
  override def loadTextData(spark: SparkSession,
                            options: Map[String, String],
                            filePaths: Seq[String]): Dataset[Product] = {

    import spark.implicits._
    val inputRecords = spark.read.options(options).schema(ProductType.schema()).csv(filePaths: _*)
    val timestampUdf = UDFS.optionalUtcTimestamp(List("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SS"))
    val df = Utils.transformColumns(inputRecords.toDF, timestampUdf, Seq(FLD_DATE_ON_SITE, FLD_DATE_MODIFIED): _*)
    df.as[Product]
  }
}

