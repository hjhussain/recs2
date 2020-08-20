package com.de.recs.common

import com.company.document.indexer.BaseSparkJob
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

case class PointerFile(path: String, content: String)

/**
  * Creates the pointer file based on the parameters. The file name and content of the file will be decided based on
  * parameters.
  */
object PointerFileCreator extends BaseSparkJob {

  override def run(spark: SparkSession, args: Map[String, String]) {
    val connectionString = args("connectionString")

    val container = args("container")

    val basePath = args("basePath")

    val azureClient = AzureClient(connectionString)

    val pointerFile = getPointerFile(args)

    azureClient.uploadTextFile(container, pointerFile.content, basePath + pointerFile.path)

  }

  /**
    * Generates the pointer file name and content based on the parameters
    *
    * @param args arguments
    * @return pointer file
    */
  def getPointerFile(args: Map[String, String]): PointerFile = {
    val modelName = args("modelName")
    val modelType = args("modelType")
    val store = args.getOrElse("store", "all")
    val origin = args.getOrElse("origin", "all")
    val mvt = args.getOrElse("mvt", "all")
    val retentionDays = args.getOrElse("retentionDays", "5").toInt
    val delimiter = "\t"
    val sliceStart = new DateTime(args("sliceStart"))

    val content =
      s"$modelName$delimiter$modelType$delimiter$store$delimiter$origin$delimiter$mvt$delimiter$retentionDays"
    val pathStore = getPath(store)
    val pathOrigin = getPath(origin)
    val pathMvt = getPath(mvt)
    val pathDate = sliceStart.toString("yyyyMMdd_HHmmss")

    val path = s"/$modelName/pointer_$pathDate.tsv"

    PointerFile(path, content)

  }

  private def getPath(value: String): String = if (value.equals("*")) "all" else value

}
