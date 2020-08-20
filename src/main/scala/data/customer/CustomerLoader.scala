package data.customer


import com.de.recs.common.FieldNames.{FLD_DATE_ENTERED, FLD_DATE_MODIFIED}
import data.common.{Loader, Utils}
import data.common.udfs.UDFS.optionalUtcTimestamp
import data.types.{Customer, CustomerType}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Customer Data Loader Implementation
  *
  * @since 2.0
  */
object CustomerLoader extends Loader[Customer] {

  val optionalTimestampUtc: UserDefinedFunction = optionalUtcTimestamp("yyyy-MM-dd'T'HH:mm:ss.SSSX")


  /**
    * Override this method to load csv files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param options   inputs options ie:delimiter,header
    * @return Dataset[Customer]
    */
  override def loadTextData(spark: SparkSession,
                            options: Map[String, String],
                            filePaths: Seq[String]): Dataset[Customer] = {

    spark.sqlContext.udf.register("strLen", (s: String) => s.length())
    val customers = spark.read
      .options(options)
      .schema(CustomerType.schema())
      .csv(filePaths: _*)

    customersWithTimestamp(customers)
  }

  /**
    * override this method to load Parquet files
    *
    * @param spark     SparkSession
    * @param filePaths Location of parquet file
    * @return Dataset[Customer]
    */
  override def loadParquet(spark: SparkSession, filePaths: String*): Dataset[Customer] = {
    val customers = spark.read.parquet(filePaths: _*)
    customersWithTimestamp(customers)
  }

  private def customersWithTimestamp(data: Dataset[Row]): Dataset[Customer] = {
    import data.sparkSession.implicits._
    Utils.transformColumns(data, optionalTimestampUtc, FLD_DATE_ENTERED, FLD_DATE_MODIFIED).as[Customer]
  }
}
