package uk.co.argos.pap.promotion.ingest

import com.typesafe.config.ConfigRenderOptions
import uk.co.argos.pap.platform.service.MicroService
import uk.co.argos.pap.platform.service.message.CommonMessages
import uk.co.argos.pap.promotion.ingest.factory.{ComponentFactory, ProductionConfig}
import uk.co.argos.pap.promotion.ingest.route.Routes
import uk.co.argos.pap.promotion.ingest.util.KamonMetrics

import scala.concurrent.Future

object Application extends KamonMetrics
  with MicroService
  with Routes
  with ComponentFactory
  with ProductionConfig {

  override def shutDown() = Future.successful(())

  override def health: Future[CommonMessages.Result] = healthMonitor.health

  startUp()

  logger.info("pst-ingest started")
  logger.debug(s"using config: ${rootConfig.root().render(ConfigRenderOptions.concise())}")
}
