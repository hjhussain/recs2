package com.de.recs.common

import com.de.recs.BaseSparkSuite


class PointerFileCreator$Test extends BaseSparkSuite {
  private val connString = ""


  private val argumentsAll = Map(
    "connectionString" -> connString,
    "container" -> "pointer",
    "basePath" -> "import/models",
    "modelName" -> "defaultmf",
    "modelType" -> "unittests",
    "store" -> "COM",
    "origin" -> "bag",
    "mvt" -> "mvt4",
    "retentionDays" -> "10",
    "sliceStart" -> "2017-03-24T14:30:20"
  )

  private val argumentsUseDefaults = Map(
    "connectionString" -> connString,
    "container" -> "pointer",
    "basePath" -> "import/models",
    "modelType" -> "unittests",
    "modelName" -> "defaultmf",
    "sliceStart" -> "2017-03-24T14:30:20"

  )

  test("getPointerFile should create the pointer file with defaults") {
    val pointerFile = PointerFileCreator.getPointerFile(argumentsUseDefaults)
    pointerFile.content should be("defaultmf\tunittests\tall\tall\tall\t5")
    pointerFile.path.startsWith("/defaultmf") should be(true)
    pointerFile.path.endsWith("pointer_20170324_143020.tsv") should be(true)
  }

  test("getPointerFile should create the pointer file with all parameters") {
    val pointerFile = PointerFileCreator.getPointerFile(argumentsAll)
    pointerFile.content should be("defaultmf\tunittests\tCOM\tbag\tmvt4\t10")
    pointerFile.path.startsWith("/defaultmf") should be(true)
    pointerFile.path.endsWith("pointer_20170324_143020.tsv") should be(true)
  }

  ignore("run should upload the pointer file to blob") {
    PointerFileCreator.run(sparkSession, argumentsAll)
    PointerFileCreator.run(sparkSession, argumentsUseDefaults)
  }
}
