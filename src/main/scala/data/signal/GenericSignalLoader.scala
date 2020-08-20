package data.signal

import com.asos.datascience.framework.core.FieldNames
import com.asos.datascience.framework.core.types.{GenericSignal, GenericSignalType}
import com.asos.datascience.framework.data.common.udfs.UDFS
import com.asos.datascience.framework.data.common.{Loader, Utils}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import FieldNames._

/**
  * Generic signal Load Implementation
  *
  * @since 4.0.3
  */

object GenericSignalLoader extends Loader[GenericSignal] {


  /**
    * Override this method to load csv files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Locations to load data from
    * @param options   inputs options ie : delimiter,header
    * @return Dataset[GenericSignal]
    */
  override def loadTextData(spark: SparkSession,
                            options: Map[String, String],
                            filePaths: Seq[String]): Dataset[GenericSignal] = {
    import spark.implicits._
    val inputRecords = spark.read.options(options).schema(GenericSignalType.schema()).csv(filePaths: _*)
    val timestampUdf = UDFS.utcTimestamp(List("yyyy-MM-dd'T'HH:mm:ss"))
    val df = Utils.transformColumns(inputRecords.toDF, timestampUdf, Seq(FLD_SIGNAL_DATE, FLD_DATE_MODIFIED): _*)
    df.as[GenericSignal]
  }

  /**
    * override this method to load Parquet files into Dataset
    *
    * @param spark     SparkSession
    * @param filePaths Location of parquet files
    * @return Dataset[GenericSignal]
    */
  override def loadParquet(spark: SparkSession, filePaths: String*): Dataset[GenericSignal] = {
    import spark.implicits._
    spark.read.parquet(filePaths: _*).as[GenericSignal]
  }
}
