package data.common.csv


import data.common.Utils
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, TimestampType}


/**
  * CSV to Parquet converter. Check SchemaDefinition for details about the json schema
  */
object CSVLoader {
  val YY = "yy"
  val MM = "mm"
  val DD = "dd"
  val HH = "hh"
  val MN = "mn"

  val validPartitions = Array(YY, MM, DD, HH, MN)

  /**
    * Converts a CSV file to Parquet
    *
    * @param spark          sparkSession
    * @param csvPath        path to csv file
    * @param parquetPath    path to output parquet file(s)
    * @param schemaPath     path to json schema in blob storage
    * @param noOfPartitions no of partitions/parquet files
    * @param mode           Save mode such as Overwrite, Append etc.
    */
  def convertToParquet(spark: SparkSession, csvPath: String, parquetPath: String, schemaPath: String,
                       schemaConnString: String, noOfPartitions: Int, mode: SaveMode)
  : Unit = {
    val schema = SchemaDefinition.fromBlob(schemaConnString, schemaPath)
    val withDatePartitionsDF = load(spark, csvPath, schemaPath, schemaConnString)
    withDatePartitionsDF.coalesce(noOfPartitions).write.mode(mode).partitionBy(schema.effectivePartitionBy(): _*)
      .parquet(parquetPath)
  }

  /**
    * Loads a CSV file to according to a schema
    *
    * @param spark      sparkSession
    * @param csvPath    path to csv file
    * @param schemaPath path to json schema in blob storage
    */
  def load(spark: SparkSession, csvPath: String, schemaPath: String, schemaConnString: String)
  : DataFrame = {
    val schema = SchemaDefinition.fromBlob(schemaConnString, schemaPath)
    val nonPIIFields = schema.nonPIIFields()
    val options = Map("header" -> schema.header.toString, "delimiter" -> schema.delimiter,
      "nullValue" -> schema.nullValue, "treatEmptyValuesAsNulls" -> "true", "dateFormat" -> schema.dateFormat,
      "mode" -> schema.mode)
    val df = spark.read.options(options).schema(schema.toStructType).csv(csvPath)
    val nonPIIDF = df.select(nonPIIFields.head, nonPIIFields.tail: _*)
    val withDatePartitionsDF = addDatePartitions(nonPIIDF, schema)

    val resultDF = if (schema.changeDateFormats) {
      val dateCols = schema.dateFormatChangeCols.split(",")
      val sourceFormat = schema.sourceDateFormat
      val targetFormat = schema.targetDateFormat
      Utils.transformDateFormats(withDatePartitionsDF, dateCols, sourceFormat, targetFormat)
    } else {
      withDatePartitionsDF
    }

    resultDF
  }

  private def addDatePartitions(df: DataFrame, schema: SchemaDefinition): DataFrame = {
    if (schema.isPartionByDate) {
      val partitionByDateCol = schema.partitionByDateCol
      val dateColType = schema.fields.filter(field => field.name.equalsIgnoreCase(partitionByDateCol)).head.dataType
      val partitionByDateOn = schema.partitionByDateOn.split(",")
      partitionByDateOn.foldLeft(df)((d, field) => addIfContains(d, partitionByDateOn, partitionByDateCol, field,
        dateColType, schema.dateFormat))
    } else {
      df
    }
  }

  private def addIfContains(df: DataFrame, partitionByDateOn: Array[String], partitionByDateCol: String, field: String,
                            dateColType: String, datePattern: String)
  : DataFrame = {
    if (partitionByDateOn.contains(field)) {
      if (dateColType.equalsIgnoreCase(DateType.toString)
        || dateColType.equalsIgnoreCase(TimestampType.toString)) {
        df.withColumn(field, getDateFunc(field)(df(partitionByDateCol)))
      } else {
        df.withColumn(field, getDateFunc(field)(from_unixtime(unix_timestamp(df(partitionByDateCol), datePattern))))
      }
    } else {
      df
    }

  }

  private def getDateFunc(field: String): Column => Column = (f: Column) => {
    if (!validPartitions.contains(field)) throw new IllegalArgumentException("Invalid Date Partition field")
    try {
      field match {
        case YY => year(f).cast(StringType)
        case MM => lpad(month(f).cast(StringType), 2, "0")
        case DD => lpad(dayofmonth(f).cast(StringType), 2, "0")
        case HH => lpad(hour(f).cast(StringType), 2, "0")
        case MN => lpad(minute(f).cast(StringType), 2, "0")
      }
    } catch {
      case e: Exception => lit("null")
    }

  }


}
