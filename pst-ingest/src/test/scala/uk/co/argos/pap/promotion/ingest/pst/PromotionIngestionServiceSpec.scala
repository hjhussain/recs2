package uk.co.argos.pap.promotion.ingest.pst

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.stream.ActorMaterializer
import co.uk.argos.pap.wcs.data.client.{PromotionsClient, PromotionsLoader}
import co.uk.argos.pap.wcs.data.config.Configurations.FileDataConfig
import co.uk.argos.pap.wcs.data.load.{CachingLoader, DataLoader}
import co.uk.argos.pap.wcs.data.parse.PromotionParser
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.apache.commons.io.IOUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import uk.co.argos.pap.platform.http.ApiHttpClient.{ApiError, ApiResponse}
import uk.co.argos.pap.promotion.ingest.factory.{ServiceConfig, ThrottlingConfig}
import uk.co.argos.pap.promotion.ingest.http.{PromotionReaderHttpClient, PromotionWriterClient, PromotionWriterHttpClient}
import uk.co.argos.pap.promotion.ingest.service.PromotionService
import uk.co.argos.pap.promotions.model.{JournalPersistenceModels, RewardModels}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class PromotionIngestionServiceSpec extends FunSpec
  with ScalaFutures
  with Matchers
  with MockitoSugar

  with BeforeAndAfterAll {

  def pstClient(resourcePath: String = "/promotions/PromotionSet-10.xml"): PromotionsClient = new PromotionsClient {
    override implicit def executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    trait LocalFileDataLoader extends DataLoader[Array[Byte], FileDataConfig] {
      override def load(dataConfig: FileDataConfig): Future[Array[Byte]] = {
        Future.successful(if (dataConfig.data.name.contains("MONDAY"))
          IOUtils.toByteArray(getClass.getResource(resourcePath))
        else
          IOUtils.toByteArray(getClass.getResource("/promotions/EmptyPromotionSet.xml")))
      }
    }

    override def promotionsLoader: PromotionsLoader[FileDataConfig] =
      new PromotionsLoader[FileDataConfig]()(scala.concurrent.ExecutionContext.Implicits.global)
        with LocalFileDataLoader
        with CachingLoader[Array[Byte], FileDataConfig]
        with PromotionParser[FileDataConfig] {
      }
  }

  def startPromotionMockServer() = {
    val host = "localhost"
    val server = new WireMockServer(wireMockConfig().dynamicPort())

    server.start()
    val port = server.port()
    WireMock.configureFor(host, port)

    (server, ServiceConfig(s"http://$host:$port", "promotionsService"))
  }


  def setupWireMockToHandleNewPromotionRequests(): Unit = {
    promotionWireMockServer.stubFor(
      get(urlPathEqualTo("/api/promotions"))
        .willReturn(aResponse()
          .withHeader("Content-Type", "application/json")
          .withBody(TestData.promotions)
          .withStatus(200)))

    promotionWireMockServer.stubFor(get(urlPathMatching("/api/segments/product/.*"))
      .willReturn(aResponse()
        .withHeader("Content-Type", "application/json")
        .withStatus(404)))

    promotionWireMockServer.stubFor(post(urlPathMatching("/api/segments/product"))
      .willReturn(aResponse()
        .withHeader("Content-Type", "application/text")
        .withBody("ok")
        .withStatus(200)))

    promotionWireMockServer.stubFor(post(urlPathMatching("/api/promotion/line"))
      .willReturn(aResponse()
        .withHeader("Content-Type", "application/json")
        .withBody("ok")
        .withStatus(200)))
  }

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
  implicit val system: ActorSystem = ActorSystem("IngestTests")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val (promotionWireMockServer, serverConfig) = startPromotionMockServer()

  val promotionWriterClient = new PromotionWriterHttpClient(
    (httpRequest: HttpRequest) => Http(system).singleRequest(httpRequest), serverConfig)

  val promotionReaderClient = new PromotionReaderHttpClient(
    (httpRequest: HttpRequest) => Http(system).singleRequest(httpRequest), serverConfig)

  val promotionService = PromotionService(promotionWriterClient, promotionReaderClient)


  override def afterAll {
    promotionWireMockServer.stop()
    system.terminate
  }


  describe("PST Ingest Tests") {

    it("PromotionIngestionService.removeUnsupported should remove  Offsite Discounts correctly") {
      whenReady(PromotionIngestionService.loadPstPromotions(pstClient("/promotions/PromotionSet.xml")), timeout(6 seconds)) { psts =>
        val result = PromotionIngestionService.removeUnsupported(psts)

        result.size shouldBe 1
      }
    }

    ignore("PromotionIngestionService.ingestPstPromotions should ingest new promotions correctly") {
      setupWireMockToHandleNewPromotionRequests()

      val promotionIngestion = PromotionIngestionService(pstClient(), promotionService, ThrottlingConfig(10, 10, 10, 10))

      val futureResult = promotionIngestion.ingestPstPromotions()
      whenReady(futureResult, timeout(10 seconds)) { result =>
        result shouldBe IngestResult(successes = 10, failures = 0)
      }
    }

    ignore("PromotionIngestionService.ingestPstPromotions should handle failures when ingesting new promotions") {
      setupWireMockToHandleNewPromotionRequests()

      val failHalfTheTimeClient = new PromotionWriterClient() {
        val ref = new AtomicInteger(0)

        override def createPromotion(promotion: JournalPersistenceModels.Promotion): Future[ApiResponse[String, String]] = {
          if(ref.getAndIncrement() % 2 == 0)
            Future(Left(ApiError("", StatusCodes.BadRequest,None)))
          else
            promotionWriterClient.createPromotion(promotion)
        }

        override def createSegment(segment: JournalPersistenceModels.Segment): Future[ApiResponse[String, String]] =
          promotionWriterClient.createSegment(segment)

        override def getSegment(id: String): Future[ApiResponse[JournalPersistenceModels.ProductSegment, String]] =
          promotionWriterClient.getSegment(id)

        override def addSegmentItems(segment: JournalPersistenceModels.Segment, items: Set[RewardModels.SkuItem]): Future[ApiResponse[JournalPersistenceModels.ProductSegment, String]] =
          promotionWriterClient.addSegmentItems(segment, items)

        override def removeSegmentItems(segment: JournalPersistenceModels.Segment, items: Set[RewardModels.SkuItem]): Future[ApiResponse[JournalPersistenceModels.ProductSegment, String]] =
          promotionWriterClient.removeSegmentItems(segment, items)
      }

      val promotionService = PromotionService(failHalfTheTimeClient, promotionReaderClient)

      val promotionIngestion = PromotionIngestionService(pstClient(), promotionService, ThrottlingConfig(10, 10, 10, 10))

      val futureResult = promotionIngestion.ingestPstPromotions()
      whenReady(futureResult, timeout(10 seconds)) { result =>
        result shouldBe IngestResult(successes = 5, failures = 5)
      }
    }
  }

}
