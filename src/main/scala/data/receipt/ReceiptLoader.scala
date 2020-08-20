package data.receipt

import com.asos.datascience.framework.core.FieldNames.{FLD_DATE_ENTERED, FLD_DATE_MODIFIED}
import com.asos.datascience.framework.core.types.{Receipt, ReceiptType}
import com.asos.datascience.framework.data.common.udfs.UDFS
import com.asos.datascience.framework.data.common.{Loader, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * Implementation of Receipts Data Loader
  *
  * @since 2.0
  */
object ReceiptLoader extends Loader[Receipt] {

  /**
    * Loads Receipts data from parquet files
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
    * Loads Receipts data from text files with default number of partitions
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
      .schema(ReceiptType.schema())
      .load(filePath)
  }

  /**
    * Loads Receipts data from text files with user defined number of partitions
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
    * Loads Receipts data from parquet files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Location of parquet file
    * @return Dataset[Receipt]
    */
  override def loadParquet(spark: SparkSession, filePaths: String*): Dataset[Receipt] = {
    val receipts = spark.read.parquet(filePaths: _*)

    receiptsWithTimestamp(receipts)
  }

  /**
    * Loads Receipts data from text files into Dataset
    *
    * @param spark     SparkSession
    * @param filePath  Location of File
    * @param delimiter Delimiter of File
    * @return Dataset[Receipt]
    * @deprecated use version with varargs
    */
  @deprecated
  override def loadText(spark: SparkSession, filePath: String, delimiter: String): Dataset[Receipt] = {
    loadTextData(spark, delimiter, filePath)
  }

  /**
    * Override this method to load csv files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param options   inputs options ie:delimiter,header
    * @return Dataset[Receipt]
    */
  override def loadTextData(spark: SparkSession,
                            options: Map[String, String],
                            filePaths: Seq[String]): Dataset[Receipt] = {
    val receipts = spark.read
      .options(options)
      .schema(ReceiptType.schema())
      .csv(filePaths: _*)

    receiptsWithTimestamp(receipts)
  }

  private def receiptsWithTimestamp(data: Dataset[Row]) = {
    import data.sparkSession.implicits._

    val utcTimestamp = UDFS.utcTimestamp(List("yyyy-MM-dd HH:mm:ss.n", "yyyy-MM-dd HH:mm:ss"))
    Utils.transformColumns(data, utcTimestamp, FLD_DATE_ENTERED, FLD_DATE_MODIFIED).as[Receipt]
  }
}

