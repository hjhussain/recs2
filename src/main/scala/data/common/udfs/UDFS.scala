package data.common.udfs

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset}

import data.common.JobUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.util._

/**
  * Collection of UDFS
  *
  * @since 3.2.0
  */

object UDFS {

  /**
    * Return a function that parses a String with the given format into a Timestamp in UTC timezone
    *
    * @param format date format
    * @return UTC timestamp as an Optional
    */
  def optionalUtcTimestamp(format: String): UserDefinedFunction = {
    lazy val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
      .withZone(ZoneId.of("UTC"))

    udf((s: String) => convertToTimestamp(defaultFormatter, s))
  }

  /**
    * Return a function that parses a String with the given format into a Timestamp in UTC timezone
    *
    * @param format date format
    * @return UTC timestamp
    */
  def utcTimestamp(format: String): UserDefinedFunction = {
    lazy val defaultFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
      .withZone(ZoneId.of("UTC"))

    udf((s: String) => {
      convertToTimestamp(defaultFormatter, s).getOrElse(throw new RuntimeException(s"Invalid date time format $s"))
    })
  }

  /**
    * Return a function that parses a String with any of the given formats into a Timestamp in UTC timezone
    *
    * @param formats possible date formats. first successful will be used
    * @return UTC timestamp
    */
  def utcTimestamp(formats: List[String]): UserDefinedFunction = {
    lazy val formatters = dateFormatters(formats)

    udf((s: String) => {
      Try(JobUtils.strToTimestamp(s)) match {
        case Success(dt) => dt
        case _ => parse(formatters, s.trim) match {
          case Some(dt) => dt
          case _ => throw new RuntimeException(s"Cannot parse date=$s Supported formats=$formats")
        }
      }
    })
  }

  /**
    * Return a function that parses a String with any of the given formats into a Timestamp in UTC timezone
    *
    * @param formats possible date formats. first successful will be used
    * @return UTC timestamp as an Optional
    */
  def optionalUtcTimestamp(formats: List[String]): UserDefinedFunction = {
    lazy val formatters = dateFormatters(formats)

    val f = (s: String) => if (s != null && s.nonEmpty) {
      Try(Some(JobUtils.strToTimestamp(s))) match {
        case Success(dt) => dt
        case _ => parse(formatters, s.trim) match {
          case Some(dt) => Some(dt)
          case _ => throw new RuntimeException(s"Cannot parse date=$s Supported formats=$formats")
        }
      }
    } else {
      None
    }
    udf(f)
  }

  private def dateFormatters(formats: List[String]): List[DateTimeFormatter] = {
    formats.map(format => DateTimeFormatter.ofPattern(format).withZone(ZoneId.of("UTC")))
  }

  private def parse(formatters: Seq[DateTimeFormatter], s: String): Option[Long] = {
    if (formatters.isEmpty) {
      return None
    }
    Try(Some(LocalDateTime.parse(s, formatters.head).toInstant(ZoneOffset.UTC).toEpochMilli)) match {
      case Success(result) => result
      case _ => parse(formatters.tail, s)
    }
  }

  private def convertToTimestamp(defaultFormatter: DateTimeFormatter, s: String) = {
    Try(Some(LocalDateTime.parse(s.trim, defaultFormatter).toInstant(ZoneOffset.UTC).toEpochMilli))
      .getOrElse(convertToLong(s))
  }

  private def convertToLong(s: String) = {
    Try(s.toLong) match {
      case Success(v) => Some(v)
      case _ => None
    }
  }

}
