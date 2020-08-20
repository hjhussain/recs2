package data.common.csv

import com.de.recs.common.AzureClient
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.types._


/**
  * Represents a Schema Definition defined in the below json schema and converts it to a StructType required by
  * Spark<br/>
  * <code>
  * <pre>
  * {
  * "schemaDefinition":{
  * "name":"TestSchema",
  * "fields":[
  * {
  * "name":"CustomerId",
  * "dataType":"LongType",
  * "nullable":true
  * },
  * {
  * "name":"ProductId",
  * "dataType":"LongType",
  * "nullable":true
  * },
  * {
  * "name":"Email",
  * "dataType":"StringType",
  * "nullable":true
  * },
  * {
  * "name":"CreatedOn",
  * "dataType":"DateType",
  * "nullable":true
  * }
  * ],
  * "delimiter":",",
  * "header":true,
  * "nullValue":"",
  * "dateFormat":"yyyy-MM-dd HH:mm:ss.S",
  * "piiFields":"Email",
  * "partitionByCol":"",
  * "partitionByDateCol":"CreatedOn",
  * "partitionByDateOn":"yy,mm,dd",
  * "mode":"PERMISSIVE"
  * "changeDateFormats":true,
  * "targetDateFormat": "M/dd/yyyy H:mm:ss a",
  * "dateFormatChangeCols":"EVENT_CAPTURED_DT"
  * }
  * }
  * </pre>
  * </code>
  */

class SchemaDefinition protected(val name: String, val fields: Array[SchemaField], val delimiter: String,
                                 val header: Boolean, val nullValue: String, val dateFormat: String,
                                 val piiFields: String, val partitionByCol: String, val partitionByDateCol: String,
                                 val partitionByDateOn: String, val mode: String, val changeDateFormats: Boolean,
                                 val targetDateFormat: String, val sourceDateFormat: String,
                                 val dateFormatChangeCols: String) {
  val VALID_MODES = Array("PERMISSIVE", "DROPMALFORMED", "FAILFAST")

  /**
    * Converts this schema definition to a StructType
    *
    * @return
    */
  def toStructType: StructType = {
    val root = new StructType()
    val results = fields.foldLeft(root)((r, field) => addField(r, field))
    results
  }

  def nonPIIFields(): List[String] = {
    val piis = piiFields.split(",")
    val allFields = fields.map(field => field.name).toList
    allFields.filter(field => !piis.contains(field))
  }

  def effectivePartitionBy(): List[String] = {
    if (!partitionByDateCol.isEmpty) {
      partitionByDateOn.split(",").toList
    } else {
      partitionByCol.split(",").toList
    }
  }

  def isPartionByDate: Boolean = {
    !partitionByDateCol.isEmpty
  }

  def isPartionByOther: Boolean = {
    !partitionByCol.isEmpty
  }

  private def addField(root: StructType, field: SchemaField): StructType = {
    root.add(StructField(field.name, getType(field), field.nullable))
  }

  private def validateSchema(): Unit = {
    val root = toStructType
    // validate the partition details
    if (!partitionByCol.isEmpty && !partitionByDateCol.isEmpty) {
      throw new IllegalArgumentException("Only one of 'partitionByCol' or 'partitionByDateCol' should be provided")
    }
    if (!partitionByCol.isEmpty && !nonPIIFields().contains(partitionByCol)) {
      throw new IllegalArgumentException(s"$partitionByCol is not a valid field for 'partitionByCol'")
    }
    if (!partitionByDateCol.isEmpty && partitionByDateOn.isEmpty) {
      throw new IllegalArgumentException("'partitionByDateOn' is required when 'partitionByDateCol' is provided.")
    }
    if (!partitionByDateOn.isEmpty) {
      if (!nonPIIFields().contains(partitionByDateCol)) {
        throw new IllegalArgumentException(s"$partitionByDateCol is not a valid field for 'partitionByDateCol'")
      }
      val fields = partitionByDateOn.split(",")
      val validFields = Array("yy", "mm", "dd", "hh", "mn")
      val result = fields.forall(field => validFields.contains(field))
      if (!result) {
        throw new IllegalArgumentException("Only yy,mm,dd,hh,mn are supported for 'partitionByDateOn'")
      }
    }
    if (mode.isEmpty) {
      throw new IllegalArgumentException("mode is required.")
    }
    if (!mode.isEmpty && !VALID_MODES.contains(mode)) {
      throw new IllegalArgumentException("mode should be one of " + VALID_MODES.mkString(","))
    }
    if (changeDateFormats) {
      if (sourceDateFormat.isEmpty) {
        throw new IllegalArgumentException("sourceDateFormat should not be empty when changeDateFormats is true")
      }
      if (targetDateFormat.isEmpty) {
        throw new IllegalArgumentException("targetDateFormat should not be empty when changeDateFormats is true")
      }
      if (dateFormatChangeCols.isEmpty) {
        throw new IllegalArgumentException("dateFormatChangeCols should not be empty when changeDateFormats is true")
      }
    }

  }

  protected def getType(field: SchemaField): DataType = {
    field.dataType match {
      case "ByteType" => ByteType
      case "ShortType" => ShortType
      case "IntegerType" => IntegerType
      case "LongType" => LongType
      case "FloatType" => FloatType
      case "DoubleType" => DoubleType
      case "StringType" => StringType
      case "TimestampType" => TimestampType
      case "DateType" => DateType
      case "BooleanType" => BooleanType
      case _ => throw new IllegalArgumentException(field.dataType + " is not a valid type")
    }

  }

}

/**
  * Companion object for loading the schema
  */
object SchemaDefinition {
  implicit val formats = DefaultFormats

  /**
    * Loads the schema from Json String
    *
    * @param json json String
    * @return schema
    */
  def fromJsonString(json: String): SchemaDefinition = {
    try {
      val jObject = parse(json)
      val schema = jObject.children.head.extract[SchemaDefinition]
      schema.validateSchema()
      schema
    } catch {
      case e: Exception => throw new IllegalArgumentException(e.getMessage)
    }
  }

  /**
    * Loads the schema from a jso file stored in Blob storage
    *
    * @param connectionString connectionString
    * @param path             file path
    * @return schema
    */
  def fromBlob(connectionString: String, path: String): SchemaDefinition = {
    try {
      val azureClient = AzureClient(connectionString)
      val text = azureClient.readTextFile(path)
      fromJsonString(text)
    } catch {
      case e: Exception => throw new IllegalArgumentException(e.getMessage)
    }
  }
}


/**
  * Represents a field in the schema
  *
  * @param name     name
  * @param dataType dataType
  * @param nullable nullable
  */
case class SchemaField(name: String, dataType: String, nullable: Boolean)
