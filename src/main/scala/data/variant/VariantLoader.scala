package data.variant

import com.asos.datascience.framework.core.FieldNames.FLD_DATE_MODIFIED
import com.asos.datascience.framework.core.types.{Variant, VariantType}
import com.asos.datascience.framework.data.common.udfs.UDFS
import com.asos.datascience.framework.data.common.{Loader, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/** Variants Data Loader
  *
  * @since 2.0
  */
object VariantLoader extends Loader[Variant] {

  /**
    * Loads Variants data from parquet files
    *
    * @param sqlContext SQL Context
    * @param filePath   Location of File
    * @return DataFrame
    * @deprecated use version that accepts a SparkSession
    */
  @deprecated
  override def loadParquet(sqlContext: SQLContext, filePath: String): DataFrame = {
    sqlContext.read.parquet(filePath)
  }

  /**
    * Loads Variant data from text files with default number of partitions
    *
    * @param sc        Spark Context
    * @param filePath  Location of File
    * @param delimiter Delimiter of File
    * @return DataFrame
    * @deprecated use version that accepts a SparkSession
    */
  @deprecated
  override def loadText(sc: SparkContext, filePath: String, delimiter: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("nullValue", "\\N")
      .option("delimiter", delimiter)
      .schema(VariantType.schema())
      .load(filePath)
  }

  /**
    * Loads Variants data from text files with user defined number of partitions
    *
    * @param sc             Spark Context
    * @param filePath       Location of File
    * @param delimiter      Delimiter of File
    * @param noOfPartitions no Of Partitions
    * @return DataFrame
    * @deprecated use version that accepts a SparkSession
    */
  @deprecated
  override def loadText(sc: SparkContext, filePath: String, delimiter: String, noOfPartitions: Int): DataFrame = {
    loadText(sc, filePath, delimiter).repartition(noOfPartitions)
  }

  /**
    * Loads Variants data from Parquet files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Location of parquet files
    * @return Dataset[Variant]
    */
  override def loadParquet(spark: SparkSession, filePaths: String*): Dataset[Variant] = {
    import spark.implicits._
    spark.read.parquet(filePaths: _*).as[Variant]
  }

  /**
    * Loads Variants data from Text files into Dataset
    *
    * @param spark     SparkSession
    * @param filePath  Location of File
    * @param delimiter Delimiter of File
    * @return Dataset[Variant]
    * @deprecated use version with varargs
    */
  @deprecated
  override def loadText(spark: SparkSession, filePath: String, delimiter: String): Dataset[Variant] = {
    loadTextData(spark, delimiter, filePath)
  }

  /**
    * Override this method to load csv files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param options   inputs options ie:delimiter,header
    * @return Dataset[Variant]
    */
  override def loadTextData(spark: SparkSession,
                            options: Map[String, String],
                            filePaths: Seq[String]): Dataset[Variant] = {
    val variantsData = spark.read
      .options(options)
      .schema(VariantType.schema())
      .csv(filePaths: _*)
    variantsWithTimestamp(spark, variantsData)
  }

  private def variantsWithTimestamp(spark: SparkSession, data: Dataset[Row]) = {
    import spark.implicits._
    val utcTimestampOptional = UDFS.optionalUtcTimestamp(List("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS"))
    Utils.transformColumns(data, utcTimestampOptional, FLD_DATE_MODIFIED).as[Variant]
  }
}


