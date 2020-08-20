package data.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Utility class for handling data
  */
object Utils {

  /**
    * Transform one date format to another
    *
    * @param sourceDF     source data frame
    * @param dateCols     an array of date columns
    * @param sourceFormat source date Format
    * @param targetFormat target date Format
    * @return data frame
    */
  def transformDateFormats(sourceDF: DataFrame, dateCols: Array[String], sourceFormat: String,
                           targetFormat: String): DataFrame = {
    dateCols.foldLeft[DataFrame](sourceDF)((df, col)
    => df.withColumn(col, from_unixtime(unix_timestamp(df(col), sourceFormat), targetFormat)))
  }

  /**
    * Transform the specified columns in the given data frame with the supplied udf
    *
    * @param data        source data frame
    * @param udf         user defined function to use for conversion
    * @param columnNames column names to apply the function to
    * @return
    */
  def transformColumns(data: DataFrame,
                       udf: UserDefinedFunction,
                       columnNames: String*): DataFrame = {

    columnNames.foldLeft(data)((df, name) => df.withColumn(name, udf(data(name)).as(name)))
  }
}
