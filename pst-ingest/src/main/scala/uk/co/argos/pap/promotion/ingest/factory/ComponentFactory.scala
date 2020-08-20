package uk.co.argos.pap.promotion.ingest.factory

import java.net.URI

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.Timeout
import co.uk.argos.pap.wcs.data.client.{PromotionsClient, PromotionsLoader}
import co.uk.argos.pap.wcs.data.config.Configurations.FileDataConfig
import co.uk.argos.pap.wcs.data.config.MainConfig
import co.uk.argos.pap.wcs.data.load.{AzureDataLoader, CachingLoader, DataLoader}
import co.uk.argos.pap.wcs.data.parse.PromotionParser
import org.apache.commons.io.IOUtils
import uk.co.argos.pap.promotion.ingest.http.{PromotionReaderHttpClient, PromotionWriterHttpClient}
import uk.co.argos.pap.promotion.ingest.pst.{IngestResult, PromotionIngestionService}
import uk.co.argos.pap.promotion.ingest.route.HealthMonitor
import uk.co.argos.pap.promotion.ingest.service.PromotionService
import uk.co.argos.pap.promotion.ingest.util.RunUtil

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

trait ComponentFactory {
  self: ConfigProvider =>

  import scala.concurrent.duration._

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer
  implicit lazy val executionContext: ExecutionContext = system.dispatcher
  implicit lazy val actorTimeout: Timeout = 15 minute
  implicit lazy val atMost: FiniteDuration = actorTimeout.duration

  lazy val healthMonitor = new HealthMonitor()(system.dispatcher, 10.seconds)

  lazy val promotionWriterClient = new PromotionWriterHttpClient(
    (httpRequest: HttpRequest) => Http(system).singleRequest(httpRequest),
    promotionServiceConfig)(materializer, system, RunUtil.boundedExecutionContext())

  lazy val promotionReaderClient = new PromotionReaderHttpClient(
    (httpRequest: HttpRequest) => Http(system).singleRequest(httpRequest),
    orchestrationServiceConfig)

  val azureDataLoader = new AzureDataLoader[FileDataConfig] {
    override val azureConnection: String = MainConfig.azureConfig.connection
  }

  trait PromotionDataLoader extends DataLoader[Array[Byte], FileDataConfig] {
    override def load(dataConfig: FileDataConfig): Future[Array[Byte]] = {
      if (ingestConfig.source.startsWith("file:///"))
        Future(IOUtils.toByteArray(new URI(ingestConfig.source)))
      else
        azureDataLoader.load(dataConfig)
    }
  }

  val promotionsClient: PromotionsClient = new PromotionsClient {
    override implicit def executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    override def promotionsLoader: PromotionsLoader[FileDataConfig] =
      new PromotionsLoader[FileDataConfig]()(scala.concurrent.ExecutionContext.Implicits.global)
        with PromotionDataLoader
        with CachingLoader[Array[Byte], FileDataConfig]
        with PromotionParser[FileDataConfig] {
      }
  }

  val promotionService = PromotionService(promotionWriterClient, promotionReaderClient)
  val ingestPromotions = PromotionIngestionService(promotionsClient, promotionService, throttlingConfig)

  def ingestPstPromotions: () => Future[IngestResult] = () => ingestPromotions.ingestPstPromotionsIfNoIngestionInProgress()


}