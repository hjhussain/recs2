package data.common

import org.apache.spark.sql.{ Dataset, SparkSession}

/**
  * Interface for loading data
  *
  * @since 4.0.2
  */
trait Loader[T]  {

  /**
    * Override this method to load Text files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param delimiter Delimiter of File
    * @return Dataset[T]
    */
  def loadTextData(spark: SparkSession, delimiter: String, filePaths: String*): Dataset[T] = {
    loadTextData(spark, Map("delimiter" -> delimiter, "header" -> "false", "nullValue" -> "\\N"), filePaths)
  }

  /**
    * Override this method to load csv files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param options   inputs options ie:delimiter,header
    * @return Dataset[T]
    */
  def loadTextData(spark: SparkSession,
                  options: Map[String, String],
                  filePaths: Seq[String]): Dataset[T]

  /**
    * override this method to load Parquet files into Dataset
    *
    * @param spark    SparkSession
    * @param filePaths Location of parquet files
    * @return Dataset[T]
    */
  def loadParquet(spark: SparkSession, filePaths: String*): Dataset[T]
}

trait TextLoader[T] extends Loader[T]
trait ParquetLoader[T] extends Loader[T]
