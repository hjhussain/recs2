package data.common

import java.time._
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * Holds common utility functions
  *
  * @since 4.0.6
  */
object JobUtils {


  private val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    .withZone(ZoneId.of("UTC"))

  /**
    * Convert string date to LocalDateTime
    *
    * @param str input date
    * @return LocalDateTime
    */
  def strToLocalDateTime(str: String): LocalDateTime = {
    val s = (if (str.trim.contains(".")) str.substring(0, str.indexOf(".")) else str).trim.replaceAll(" ", "T")
    LocalDateTime.parse(s, defaultFormatter)
  }

  /**
    * Convert string date to timestamp
    *
    * @param str input date with following formats ("yyyy-MM-dd'T'HH:mm:ss" or "yyyy-MM-dd HH:mm:ss")
    * @return timestamp
    */
  def strToTimestamp(str: String): Long = {
    strToLocalDateTime(str).toInstant(ZoneOffset.UTC).toEpochMilli
  }

  /**
    * convert timestamp to string date
    *
    * @param millis
    * @return date as string
    */
  def timestampToStr(millis: Long): String = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC).format(defaultFormatter)
  }

  /**
    * convert timestamp to string date
    *
    * @param millis
    * @param formatter date formatter, by default is "yyyy-MM-dd'T'HH:mm:ss"
    * @return date as string
    */
  def timestampToStr(millis: Long, formatter: DateTimeFormatter = defaultFormatter): String = {
    LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC).format(formatter)
  }

  /**
    * This is used to create a list of dataset time ranges for which to generate features.
    *
    * @param sliceStart     a job's starting timestamp as string
    * @param outputFeatures comma delimited list of dataset time ranges to generate as in the following example:
    *                       outputFeatures=current:0:365,historical:365:730
    *                       The above will be used to create 2 data sets of a feature and saved as:
    *                       ../current : data from sliceStart to sliceStart-365 days (i.e. last year from sliceStart)
    *                       ../historical : data from sliceStart-365 to sliceStart-730 days (i.e the year before last)
    *                       for data sets that will be used to generate labels, add the the label as a second value:
    *                       e.g. "current:labels:0:365" will generate labels dataset as well as current features
    * @return List of Tuple of (datasetName, labels dataSetName or empty string for no labels,  startTime, endTime)
    */
  def calculateTimes(sliceStart: String, outputFeatures: String): Array[(String, String, Long, Long)] = {
    val millisInDays = TimeUnit.DAYS.toMillis _
    val sliceStartTime = strToTimestamp(sliceStart)

    outputFeatures.split(',').map(s => {
      s.split(':') match {
        case Array(name, first, last) => {
          val from = sliceStartTime - millisInDays(last.toLong)
          val to = sliceStartTime - millisInDays(first.toLong)
          (name, "", from, to)
        }
        case Array(name, labels, first, last) => {
          val from = sliceStartTime - millisInDays(last.toLong)
          val to = sliceStartTime - millisInDays(first.toLong)
          (name, labels, from, to)
        }
        case _ => throw new RuntimeException("unsupported outputFeatures format=" + outputFeatures)
      }
    })
  }

  /**
    * concat base to the varargs to create a valid path
    *
    * @param base first part of path
    * @param more other parts
    * @return path as a string
    */
  def makePath(base: String, more: String*): String = {
    more.foldLeft(base)((acc, s) => {
      val start = if (acc.endsWith("/")) acc else acc + "/"
      val end = if (s.startsWith("/")) s.substring(1) else s
      start + end
    })
  }

  /**
    * convert datetime string to a valid path: e.g. 2017-04-01T00:00:00 to 2017/04/01
    *
    * @param datetime      string to convert
    * @param dateFormatter format of datetime which defaults to yyyy-MM-dd'T'HH:mm:ss
    * @return date as yyyy/MM/dd
    */
  def dateToPath(datetime: String,
                 dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME): String = {

    val dt = LocalDateTime.parse(datetime, dateFormatter)

    val year = dt.getYear
    val month = dt.getMonthValue
    val day = dt.getDayOfMonth
    makePath(year.toString, f"$month%02d", f"$day%02d")
  }

  /**
    * Generate an Array of date paths based on date range (only months will be included)
    *
    * @param base           base path to use when creating the path with date
    * @param startTimestamp from timestamp
    * @param endTimestamp   to timestamp
    * @param dateFormats    date formatters in path (defaults to parquet format)
    * @return Array of paths
    */
  def monthPathsFromDateRange(base: String,
                              startTimestamp: Long,
                              endTimestamp: Long,
                              dateFormats: Map[String, String] = Map("year" -> "yy=%4d",
                                "month" -> "mm=%02d",
                                "day" -> "dd=%02d")): Array[String] = {

    val from = LocalDateTime.ofInstant(Instant.ofEpochMilli(startTimestamp), ZoneId.of("UTC"))
    val to = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTimestamp), ZoneId.of("UTC"))

    val (fromDate, toDate) = if (to.isBefore(from)) (to, from) else (from, to)

    def formatInt(format: String, i: Int): String = {
      String.format(format, new Integer(i))
    }

    def globDateToPath(d: LocalDateTime): String = {
      val year = formatInt(dateFormats("year"), d.getYear)
      val month = formatInt(dateFormats("month"), d.getMonthValue)
      makePath(base, year, month, "*")
    }

    def firstDayOfMonth(d: LocalDateTime): LocalDateTime = {
      LocalDateTime.of(d.getYear, d.getMonth, 1, 0, 0, 0)
    }

    val toDateStart = firstDayOfMonth(fromDate)

    Stream.from(0)
      .map(i => toDateStart.plusMonths(i))
      .takeWhile(d => !d.isAfter(toDate))
      .map(globDateToPath)
      .distinct
      .toArray
  }


  /**
    * Generate an Array of date paths based on date range
    *
    * @param base           base path to use when creating the path with date
    * @param startTimestamp from timestamp
    * @param endTimestamp   to timestamp
    * @param dateFormats    date formatters in path (defaults to parquet format)
    * @return Array of paths
    */
  def pathsFromDateRange(base: String,
                         startTimestamp: Long,
                         endTimestamp: Long,
                         dateFormats: Map[String, String] = Map("year" -> "yy=%4d",
                           "month" -> "mm=%02d",
                           "day" -> "dd=%02d")): Array[String] = {

    val from = LocalDateTime.ofInstant(Instant.ofEpochMilli(startTimestamp), ZoneId.of("UTC"))
    val to = LocalDateTime.ofInstant(Instant.ofEpochMilli(endTimestamp), ZoneId.of("UTC"))

    val (fromDate, toDate) = if (to.isBefore(from)) (to, from) else (from, to)

    def formatInt(format: String, i: Int): String = {
      String.format(format, new Integer(i))
    }

    def dateToPath(d: LocalDateTime): String = {
      val year = formatInt(dateFormats("year"), d.getYear)
      val month = formatInt(dateFormats("month"), d.getMonthValue)
      val day = formatInt(dateFormats("day"), d.getDayOfMonth)

      makePath(base, year, month, day)
    }

    def globDateToPath(d: LocalDateTime): String = {
      val year = formatInt(dateFormats("year"), d.getYear)
      val month = formatInt(dateFormats("month"), d.getMonthValue)
      makePath(base, year, month, "*")
    }

    def firstDayOfMonth(d: LocalDateTime): LocalDateTime = {
      LocalDateTime.of(d.getYear, d.getMonth, 1, 0, 0, 0)
    }

    if (fromDate.getYear == toDate.getYear && fromDate.getMonthValue == toDate.getMonthValue) {
      Stream.from(0).map(i => fromDate.plusDays(i))
        .takeWhile(d => d.isBefore(toDate) || d.getDayOfMonth == toDate.getDayOfMonth)
        .map(dateToPath).toArray
    } else {
      val fromDateEnd = firstDayOfMonth(fromDate.plusMonths(1))
      val toDateStart = firstDayOfMonth(toDate)

      val monthsBetween = Stream.from(1)
        .map(i => fromDate.plusMonths(i))
        .takeWhile(d => d.isBefore(toDate) && d.isBefore(toDateStart))
        .map(globDateToPath)

      val firstMonthDays = Stream.from(0).map(i => fromDate.plusDays(i))
        .takeWhile(d => d.isBefore(fromDateEnd))
        .map(dateToPath)

      val lastMonthDays = Stream.from(0).map(i => toDateStart.plusDays(i))
        .takeWhile(d => d.isBefore(toDate) || d.equals(toDate))
        .map(dateToPath)

      (firstMonthDays ++ monthsBetween ++ lastMonthDays).toArray
    }
  }

  /**
    * Load all parquet data in basedPath within the given time range
    *
    * @param spark    the SparkSession
    * @param basePath base path to use when creating the path with date
    * @param fromDate from timestamp
    * @param toDate   to timestamp
    * @return DataFrame
    */
  def loadParquet(spark: SparkSession,
                  basePath: String,
                  fromDate: Long,
                  toDate: Long): DataFrame = {

    val validPaths = pathsFromDateRange(basePath, fromDate, toDate).filter(parquetExists(spark))
    if (validPaths.nonEmpty) {
      spark.read.parquet(validPaths: _*)
    } else {
      val from = Instant.ofEpochMilli(fromDate)
      val to = Instant.ofEpochMilli(toDate)
      throw new RuntimeException(s"No data exists in basePath=$basePath, for date range=($from, $to")
    }
  }

  /**
    * Load all CSV data in basePath within the given time range
    *
    * @param spark    the SparkSession
    * @param basePath base path to use when creating the path with date
    * @param fromDate from timestamp
    * @param toDate   to timestamp
    * @return DataFrame
    */
  def loadCsv[T](spark: SparkSession,
                 loader: Loader[T],
                 basePath: String,
                 fromDate: Long,
                 toDate: Long,
                 delimiter: String = "\t"): Dataset[T] = {

    val dateInPathFormat = Map("year" -> "%4d", "month" -> "%02d", "day" -> "%02d")
    val validPaths = pathsFromDateRange(basePath, fromDate, toDate, dateInPathFormat).filter(csvExists(spark))
    if (validPaths.nonEmpty) {
      loader.loadTextData(spark, delimiter, validPaths: _*)
    } else {
      val from = Instant.ofEpochMilli(fromDate)
      val to = Instant.ofEpochMilli(toDate)
      throw new RuntimeException(s"No data exists in basePath=$basePath, for date range=($from, $to")
    }
  }

  /**
    * Check parquet file exists in storage system
    *
    * @param spark the SparkSession
    * @param path  the path to check
    * @return true if path exists and false if not
    */
  def parquetExists(spark: SparkSession)(path: String): Boolean = {
    Try(spark.read.parquet(path)).isSuccess
  }

  /**
    * Check Text file exists in storage system
    *
    * @param spark the SparkSession
    * @param path  the path to check
    * @return true if path exists and false if not
    */
  def csvExists(spark: SparkSession)(path: String): Boolean = {
    Try(spark.read.csv(path)).isSuccess
  }

  /**
    * Save dataset to a parquet file
    *
    * @param df            the data set to save
    * @param path          storage path to save to
    * @param numPartitions number of partitions
    */
  def save[T](df: Dataset[T], path: String, numPartitions: Int) {
    df.coalesce(numPartitions).write.parquet(path)
  }

  /**
    * Save dataset to csv
    *
    * @param records       dataFrame to save
    * @param path          destination path
    * @param delimiter
    * @param numPartitions number of partitions
    */
  def saveText[T](records: Dataset[T], path: String, delimiter: String, numPartitions: Int) {
    records.coalesce(numPartitions).write
      .option("delimiter", delimiter)
      .option("nullValue", "\\N")
      .option("headers", "false")
      .csv(path)
  }

  /**
    * un-persist the given datasets
    *
    * @param dfs dataframes to un-persist (should only pass the cached datasets)
    */
  def unpersist[P <: Any](dfs: Dataset[P]*) {
    Try(dfs.foreach(_.unpersist()))
  }

  /**
    * Save dataFrame to parquet and CSV
    *
    * @param df          dataFrame to save
    * @param parquetPath parquet destination path
    * @param csvPath     CSV destination path
    * @param partitions  number of partitions
    * @param delimiter
    */
  def save[T](df: Dataset[T],
              parquetPath: String,
              csvPath: String,
              partitions: Int = 32,
              delimiter: String = "|") {
    df.coalesce(partitions).write.parquet(parquetPath)
    if (csvPath.nonEmpty) {
      JobUtils.saveText(df, csvPath, delimiter, partitions)
    }
  }

  /**
    * Save and repartition a dataset to parquet
    *
    * @param ds          dataset to save
    * @param path        destination path
    * @param numberOfPartition
    * @param partitioner function to apply for partition column
    */
  def saveParquetPartitioned[T](ds: Dataset[T],
                                path: String,
                                numberOfPartition: Int,
                                partitioner: Dataset[T] => (Seq[String], DataFrame)) {
    val (colNames, records) = partitioner(ds)
    records.repartition(numberOfPartition).write
      .mode(SaveMode.Append)
      .partitionBy(colNames: _*)
      .parquet(path)
  }

  /**
    * Save and repartition a dataset to parquet
    *
    * @param ds          dataset to save
    * @param path        destination path
    * @param numberOfPartition
    * @param partitioner function to apply for partition column
    */

  def saveCsvPartitioned[T](ds: Dataset[T],
                            path: String,
                            numberOfPartition: Int,
                            delimiter: String = "|",
                            partitioner: Dataset[T] => (Seq[String], DataFrame)) {
    val (colNames, records) = partitioner(ds)
    records.repartition(numberOfPartition).write
      .mode(SaveMode.Append)
      .partitionBy(colNames: _*)
      .option("delimiter", delimiter)
      .option("headers", "false")
      .csv(path)
  }

  /**
    * Partition by yyyy/mm/dd
    *
    * @param dateColumn
    * @return func which take dataset as input => tupple
    */
  def datePartitioner[T](dateColumn: String): Dataset[T] => (Seq[String], DataFrame) = {
    (ds: Dataset[T]) => {
      val df: DataFrame = ds.withColumn("yy", substring(ds(dateColumn), 1, 4))
        .withColumn("mm", substring(ds(dateColumn), 6, 2))
        .withColumn("dd", substring(ds(dateColumn), 9, 2))
      (Seq("yy", "mm", "dd"), df)
    }
  }


  /**
    * Partition by a timestamp columns
    *
    * @param dateColumn
    * @return func which take dataset as input => tupple
    */
  def timestampPartitioner[T](dateColumn: String): Dataset[T] => (Seq[String], DataFrame) = {
    val timestampToStrUdf = udf((millis: Long) => timestampToStr(millis, defaultFormatter))

    (ds: Dataset[T]) => {
      val colName = "timestampPartitioner_Col"
      val withPartitionCol = ds.withColumn(colName, timestampToStrUdf(ds(dateColumn)))
      val r = datePartitioner[Row](colName)(withPartitionCol)
      (r._1, r._2.drop(colName))
    }
  }
}

