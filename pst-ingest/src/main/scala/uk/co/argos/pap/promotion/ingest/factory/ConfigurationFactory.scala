package uk.co.argos.pap.promotion.ingest.factory

import co.uk.argos.pap.wcs.data.config.Configurations.ConfigurationParseException
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader
import pureconfig.generic.auto._

final case class ServiceConfig(host: String, serviceName: String)

final case class ThrottlingConfig(requestsPerNSeconds: Int,
                                  bufferSize: Int,
                                  requests: Int,
                                  maxBurst: Int)
final case class IngestConfig(source: String)

trait ConfigProvider {
  def parseConfig[ConfigType](config: Config)(implicit reader: ConfigReader[ConfigType]): ConfigType = {
    pureconfig.loadConfig[ConfigType](config) match {
      case Left(ex) => throw ConfigurationParseException(ex)
      case Right(resultConfig) => resultConfig
    }
  }

  def promotionServiceConfig: ServiceConfig
  def orchestrationServiceConfig: ServiceConfig
  def throttlingConfig: ThrottlingConfig
  def ingestConfig:IngestConfig
}

trait ProductionConfig extends ConfigProvider {
  lazy val rootConfig = ConfigFactory.load()

  lazy val promotionServiceConfig: ServiceConfig =
    parseConfig[ServiceConfig](rootConfig.getConfig("http.client.promotion-service"))

  lazy val orchestrationServiceConfig: ServiceConfig =
    parseConfig[ServiceConfig](rootConfig.getConfig("http.client.orchestration-service"))
  lazy val throttlingConfig: ThrottlingConfig =
    parseConfig[ThrottlingConfig](rootConfig.getConfig("http.client.throttle"))

  lazy val ingestConfig: IngestConfig =
    parseConfig[IngestConfig](rootConfig.getConfig("ingest"))
}

