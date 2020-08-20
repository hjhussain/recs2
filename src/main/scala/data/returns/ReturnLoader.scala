package data.returns

import com.asos.datascience.framework.core.FieldNames._
import com.asos.datascience.framework.core.types.{Return, ReturnType}
import com.asos.datascience.framework.data.common.udfs.UDFS
import com.asos.datascience.framework.data.common.{Loader, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * Implementation of Returns Data Loader
  *
  * @since 2.0
  */
object ReturnLoader extends Loader[Return] {

  /**
    * Loads Returns data from parquet files
    *
    * @param sqlContext SQL Context
    * @param filePath   Location of File
    * @return Returns DataFrame
    * @deprecated use version that accepts a SparkSession
    */
  @deprecated
  override def loadParquet(sqlContext: SQLContext, filePath: String): DataFrame = {
    sqlContext.read.parquet(filePath)
  }

  /**
    * Loads Returns data from text files with default number of partitions
    *
    * @param sc        Spark Context
    * @param filePath  Location of File
    * @param delimiter Delimiter of File
    * @return Returns DataFrame
    * @deprecated use version that accepts a SparkSession
    */
  @deprecated
  override def loadText(sc: SparkContext, filePath: String, delimiter: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("nullValue", "")
      .option("nullValue", "\\N")
      .option("delimiter", delimiter)
      .schema(ReturnType.schema())
      .load(filePath)
  }

  /**
    * Loads Returns data from text files with user defined number of partitions
    *
    * @param sc             Spark Context
    * @param filePath       Location of File
    * @param delimiter      Delimiter of File
    * @param noOfPartitions no Of Partitions
    * @return Returns DataFrame
    * @deprecated use version that accepts a SparkSession
    */
  @deprecated
  override def loadText(sc: SparkContext, filePath: String, delimiter: String, noOfPartitions: Int): DataFrame = {
    loadText(sc, filePath, delimiter).repartition(noOfPartitions)
  }

  /**
    * Load Return data from one or more Parquet locations into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Location of parquet file
    * @return Dataset[Return]
    */
  override def loadParquet(spark: SparkSession, filePaths: String*): Dataset[Return] = {
    val returns = spark.read.parquet(filePaths: _*)
    returnsWithTimestamp(returns)
  }

  /**
    * Override this method to load Text files into Dataset
    *
    * @param spark     SparkSession
    * @param filePath  Location of File
    * @param delimiter Delimiter of File
    * @return Dataset[Return]
    * @deprecated use version with varargs
    */
  @deprecated
  override def loadText(spark: SparkSession, filePath: String, delimiter: String): Dataset[Return] = {
    loadTextData(spark, delimiter, filePath)
  }

  /**
    * Override this method to load csv files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param options   inputs options ie:delimiter,header
    * @return Dataset[Ruturn]
    */
  override def loadTextData(spark: SparkSession,
                            options: Map[String, String],
                            filePaths: Seq[String]): Dataset[Return] = {
    val returns = spark.read
      .options(options)
      .schema(ReturnType.schema())
      .csv(filePaths: _*)

    returnsWithTimestamp(returns)
  }

  private def returnsWithTimestamp(data: Dataset[Row]) = {
    import data.sparkSession.implicits._
    val dateFormats = List("yyyy-MM-dd HH:mm:ss.n", "yyyy-MM-dd HH:mm:ss")
    val utcTimestampOptional = UDFS.optionalUtcTimestamp(dateFormats)
    val utcTimestamp = UDFS.utcTimestamp(dateFormats)

    val df = Utils.transformColumns(data, utcTimestamp, FLD_RETURNS_DATE_KEY, FLD_RECEIPT_DATE_KEY)
    Utils.transformColumns(df, utcTimestampOptional, FLD_DATE_ENTERED, FLD_DATE_MODIFIED).as[Return]
  }
}


