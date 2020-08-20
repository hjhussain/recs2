
name := "RecsEngine"

version := (version in ThisBuild).value

scalaVersion := "2.11.8"

scalacOptions += "-target:jvm-1.8"

updateOptions := updateOptions.value.withCachedResolution(true)
resolvers += "Artifactory" at "http://datascience.northeurope.cloudapp.azure.com:8081/artifactory/sbt-local"
credentials += Credentials(new File("credentials.properties"))

val sparkVersion = "2.1.0"
val frameworkVersion = "4.0.6"
libraryDependencies ++= Seq(
  "com.eed3si9n" % "sbt-assembly_2.8.1" % "sbt0.10.1_0.6" % "provided",
  "org.apache.spark"  %% "spark-core"  % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-mllib" % sparkVersion % "provided",
  "joda-time" % "joda-time" % "2.9.2",
  "org.joda"  % "joda-convert" % "1.8",
  "commons-io" % "commons-io" % "2.5"
)

// libraries used for testing
libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "commons-io" % "commons-io" % "2.5" % "test"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


assemblyOption in assembly ~= {
  _.copy(includeScala = false)
}
parallelExecution in Test := false
test in assembly := {}
publishArtifact in Test := false

org.scalastyle.sbt.ScalastylePlugin.scalastyleConfig := file("project/scalastyle_config.xml")

publishTo := Some(Resolver.file("file", file(Path.userHome.absolutePath + "/.m2/repository")))

assemblyJarName in assembly := "RecsEngine-" + (version in ThisBuild).value + ".jar"

sbt.addArtifact(Artifact("RecsEngine-", "assembly"), sbtassembly.AssemblyKeys.assembly)