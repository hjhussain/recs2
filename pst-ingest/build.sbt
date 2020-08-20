import sbtassembly.MergeStrategy

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "uk.co.argos.pap",
      scalaVersion := "2.12.8",
      version := "0.0.1"
    )),
    name := "pst-ingest",
    libraryDependencies ++= {
      val akkaHttpVersion = "10.1.10"
      val akkaVersion = "2.6.0"
      val catsVersion = "1.6.0"
      val papAkkaHttpMicroserviceVersion = "0.0.21"
      val papPlatformVersion = "0.0.26"
      val papPromotionsDataModelVersion = "im_not_shy-SNAPSHOT"

      val papWcsDataVersion = "0.0.14"

      Seq(
        "com.typesafe.slick" %% "slick" % "3.3.0",
        //"org.slf4j" % "slf4j-nop" % "1.6.4",
        "org.slf4j" % "slf4j-api" % "1.7.29",
        "com.typesafe.slick" %% "slick-hikaricp" % "3.3.0",
        "io.scalaland" %% "chimney" % "0.3.2",
        "com.github.pureconfig" %% "pureconfig" % "0.12.1",

        "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
        "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,

        "uk.co.argos.pap" %% "promotions-data-model" % papPromotionsDataModelVersion,
        "uk.co.argos.pap" % "akka-http-microservice" % papAkkaHttpMicroserviceVersion excludeAll "com.typesafe.akka" ,
        "uk.co.argos.pap" % "platform" % papPlatformVersion excludeAll "com.typesafe.akka" ,
        //"uk.co.argos.pap" % "product-promotions-event-store-client" % papPromotionsProductClient excludeAll "com.typesafe.akka" ,
        "uk.co.argos.pap" % "wcs-data" % papWcsDataVersion,

        "org.typelevel" %% "cats-core" % catsVersion,
        "org.typelevel" %% "cats-macros" % catsVersion,
        "org.typelevel" %% "cats-kernel" % catsVersion,
        "io.kamon" %% "kamon-bundle" % "2.0.5",
        "io.kamon" %% "kamon-statsd" % "2.0.0",
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-stream" % akkaVersion,

        "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test ,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
        "org.scalatest" %% "scalatest" % "3.0.1" % Test,
        "org.mockito" % "mockito-core" % "2.23.0" % Test,
        "org.scalamock" %% "scalamock" % "4.1.0" % Test,
        "org.mockito" %% "mockito-scala-scalatest" % "1.7.1" % Test,
      "com.github.tomakehurst" % "wiremock" % "2.23.2" % Test
      )
    },
    dependencyOverrides ++= Seq(
      "uk.co.argos.pap" % "wcs-data" % "0.0.14",
      // TODO: remove this
      // "uk.co.argos.pap" %% "promotions-data-model" % "add-inlined-segment-SNAPSHOT"
    )
  )

scalacOptions += "-Ypartial-unification"

resolvers ++=
  Seq(
    Resolver.mavenLocal,
    Resolver.bintrayRepo("kamon-io", "sbt-plugins"),
    "Pap releases" at "https://nexus.deveng.systems/content/repositories/pap-releases/",
    "Pap snapshots" at "https://nexus.deveng.systems/content/repositories/pap-snapshots/"
  )

mainClass in assembly := Some("uk.co.argos.pap.promotion.ingest.Application")

def defaultMergeStrategy(s: String): MergeStrategy = s match {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api"       % "1.7.7",
  "org.slf4j" % "jcl-over-slf4j"  % "1.7.7"
).map(_.force())

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-jdk14")) }

// Use defaultMergeStrategy with a case for aop.xml
// I like this better than the inline version mentioned in assembly's README
val customMergeStrategy: String => MergeStrategy =
  s => defaultMergeStrategy(s)

assemblyMergeStrategy in assembly := customMergeStrategy

test in assembly := {}

import Append._
unmanagedSourceDirectories in Compile += baseDirectory.value / "src/generated-sources/scala"
