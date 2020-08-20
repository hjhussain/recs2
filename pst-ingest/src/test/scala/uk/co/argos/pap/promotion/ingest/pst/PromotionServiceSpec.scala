package uk.co.argos.pap.promotion.ingest.pst

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.stream.ActorMaterializer
import co.uk.argos.pap.wcs.data.client.{PromotionsClient, PromotionsLoader}
import co.uk.argos.pap.wcs.data.config.Configurations.FileDataConfig
import co.uk.argos.pap.wcs.data.load.{CachingLoader, DataLoader}
import co.uk.argos.pap.wcs.data.parse.PromotionParser
import org.apache.commons.io.IOUtils
import org.joda.time.DateTime
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{reset, times, verify, when}
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}
import uk.co.argos.pap.platform.http.ApiHttpClient.ApiError
import uk.co.argos.pap.promotion.ingest.http.{PromotionReaderClient, PromotionWriterClient}
import uk.co.argos.pap.promotion.ingest.service.{PromotionService, Successful, Unsuccessful}
import uk.co.argos.pap.promotions.domain.Identification
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Promotion}
import uk.co.argos.pap.promotions.model.PromotionModels.{DateRange, Descriptor, LineItemPromotion, OffSiteRedemption}
import uk.co.argos.pap.promotions.model.RewardModels.{SkuItem, SupplierProvidedReward}
import uk.co.argos.pap.promotions.model.SegmentModels._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class PromotionServiceSpec extends FunSpec
  with ScalaFutures
  with Matchers
  with MockitoSugar
  with MacroBasedMatchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  def pstClient(resourcePath: String = "/promotions/PromotionSet.xml"): PromotionsClient = new PromotionsClient {
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

  implicit val ec: ExecutionContext = ExecutionContext.Implicits.global
  implicit val system: ActorSystem = ActorSystem("IngestTests")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def afterAll {
    system.terminate
  }

  val mockPromotionWriterClient = mock[PromotionWriterClient]
  val mockPromotionReaderClient = mock[PromotionReaderClient]
  val promotionService = PromotionService(mockPromotionWriterClient, mockPromotionReaderClient)

  override protected def beforeEach(): Unit = {
    reset(mockPromotionWriterClient)
    reset(mockPromotionReaderClient)
  }

  describe("writePromotion Tests") {
    val unexpectedCall = new RuntimeException("unexpected call")

    def success[T](result: T) = Future.successful(Right(result))

    def fail(statusCode: StatusCode) = Future.successful(Left(ApiError("", statusCode, Some(""))))

    it("should create a new promotion with its segments") {

      val promotion = Promotion(
        Descriptor(UUID.randomUUID(), "E25137", "E25137 desc", "50.0% OFF 1 IN E25137_one IF 1 IN E25137_zero"),
        LineItemPromotion(OffSiteRedemption(SupplierProvidedReward), Qualification(MatchOperator(EQ, Count(1), IN(NamedProductSegmentDescriptor(SelfStyledNamespace("E25233_zero"), GroupNamespace("ape"), "E25233_zero", None))))),
        DateRange(DateTime.parse("2020-01-27"), DateTime.parse("2020-01-28")))

      val segments = List(productSegment("segmentName", "1", "2"))

      when(mockPromotionReaderClient.retrievePromotions())
        .thenReturn(success(List())).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.getSegment(any[String]))
        .thenReturn(fail(StatusCodes.NotFound)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.createSegment(segments.head))
        .thenReturn(success("ok")).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.createPromotion(promotion))
        .thenReturn(success("ok")).thenThrow(unexpectedCall)

      val futureResult = promotionService.writePromotion(PromotionAndSegments(promotion, segments))
      whenReady(futureResult, timeout(6 seconds)) { result =>
        result shouldBe Successful("ok")

        val segmentCaptor = ArgumentCaptor.forClass(classOf[ProductSegment])
        verify(mockPromotionWriterClient, times(1)).createSegment(segmentCaptor.capture())

        val segmentSentToBeCreated = segmentCaptor.getValue.asInstanceOf[ProductSegment]
        segmentSentToBeCreated shouldBe(segments.head)
      }
    }

    it("should update a segment then create a promotion") {
      val promotion = Promotion(
        Descriptor(UUID.randomUUID(), "E25137", "E25137 desc", "50.0% OFF 1 IN E25137_one IF 1 IN E25137_zero"),
        LineItemPromotion(OffSiteRedemption(SupplierProvidedReward), Qualification(MatchOperator(EQ, Count(1), IN(NamedProductSegmentDescriptor(SelfStyledNamespace("E25233_zero"), GroupNamespace("ape"), "E25233_zero", None))))),
        DateRange(DateTime.parse("2020-01-27"), DateTime.parse("2020-01-28")))

      val existingSegment = productSegment("segmentName", "1", "2")
      val updatedSegment = productSegment("segmentName", "2", "3", "4")
      val id = Identification.namespaceForSegment(existingSegment.descriptor).getOrElse("").toString

      val segments = List(updatedSegment)

      when(mockPromotionReaderClient.retrievePromotions())
        .thenReturn(success(List())).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.getSegment(id))
        .thenReturn(success(existingSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.addSegmentItems(segments.head, Set(SkuItem("3"), SkuItem("4"))))
        .thenReturn(success(updatedSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.removeSegmentItems(segments.head, Set(SkuItem("1"))))
        .thenReturn(success(updatedSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.createPromotion(promotion))
        .thenReturn(success("ok")).thenThrow(unexpectedCall)

      val futureResult = promotionService.writePromotion(PromotionAndSegments(promotion, segments))
      whenReady(futureResult, timeout(6 seconds)) { result =>
        result shouldBe Successful("ok")

        val segmentCaptor = ArgumentCaptor.forClass(classOf[ProductSegment])
        val itemsCaptor = ArgumentCaptor.forClass(classOf[Set[SkuItem]])

        verify(mockPromotionWriterClient, times(1)).removeSegmentItems(segmentCaptor.capture(), itemsCaptor.capture())
        val itemsSentToRemoveFromSegment = itemsCaptor.getValue.asInstanceOf[Set[SkuItem]]
        itemsSentToRemoveFromSegment shouldBe Set(SkuItem("1"))

        verify(mockPromotionWriterClient, times(1)).addSegmentItems(segmentCaptor.capture(), itemsCaptor.capture())
        val itemsSentToAddToSegment = itemsCaptor.getValue.asInstanceOf[Set[SkuItem]]
        itemsSentToAddToSegment shouldBe Set(SkuItem("3"), SkuItem("4"))
      }
    }

    it("should use an existing segment if no updates are required then create a promotion") {
      val promotion = Promotion(
        Descriptor(UUID.randomUUID(), "E25137", "E25137 desc", "50.0% OFF 1 IN E25137_one IF 1 IN E25137_zero"),
        LineItemPromotion(OffSiteRedemption(SupplierProvidedReward), Qualification(MatchOperator(EQ, Count(1), IN(NamedProductSegmentDescriptor(SelfStyledNamespace("E25233_zero"), GroupNamespace("ape"), "E25233_zero", None))))),
        DateRange(DateTime.parse("2020-01-27"), DateTime.parse("2020-01-28")))

      val existingSegment = productSegment("segmentName", "1", "2")
      val updatedSegment = productSegment("segmentName", "1", "2")
      val id = Identification.namespaceForSegment(existingSegment.descriptor).getOrElse("").toString

      val segments = List(updatedSegment)

      when(mockPromotionReaderClient.retrievePromotions())
        .thenReturn(success(List())).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.getSegment(id))
        .thenReturn(success(existingSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.createPromotion(promotion)).thenReturn(success("ok"))

      val futureResult = promotionService.writePromotion(PromotionAndSegments(promotion, segments))
      whenReady(futureResult, timeout(6 seconds)) { result =>
        result shouldBe Successful("ok")
      }
    }

    it("should reverse segment updates when writePromotion fails") {
      val promotion = Promotion(
        Descriptor(UUID.randomUUID(), "E25137", "E25137 desc", "50.0% OFF 1 IN E25137_one IF 1 IN E25137_zero"),
        LineItemPromotion(OffSiteRedemption(SupplierProvidedReward), Qualification(MatchOperator(EQ, Count(1), IN(NamedProductSegmentDescriptor(SelfStyledNamespace("E25233_zero"), GroupNamespace("ape"), "E25233_zero", None))))),
        DateRange(DateTime.parse("2020-01-27"), DateTime.parse("2020-01-28")))

      val existingSegment = productSegment("segmentName", "1", "2")
      val updatedSegment = productSegment("segmentName", "1", "2", "3", "4")
      val id = Identification.namespaceForSegment(existingSegment.descriptor).getOrElse("").toString

      val segments = List(updatedSegment)

      when(mockPromotionReaderClient.retrievePromotions())
        .thenReturn(success(List())).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.getSegment(id))
        .thenReturn(success(existingSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.addSegmentItems(segments.head, Set(SkuItem("3"), SkuItem("4"))))
        .thenReturn(success(updatedSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.createPromotion(promotion))
        .thenReturn(fail(StatusCodes.BadRequest)).thenThrow(unexpectedCall)

      val futureResult = promotionService.writePromotion(PromotionAndSegments(promotion, segments))

      whenReady(futureResult, timeout(6 seconds)) { result =>
        result shouldBe Unsuccessful("failed")

        val segmentCaptor = ArgumentCaptor.forClass(classOf[ProductSegment])
        val itemsCaptor = ArgumentCaptor.forClass(classOf[Set[SkuItem]])

        verify(mockPromotionWriterClient, times(1)).removeSegmentItems(segmentCaptor.capture(), itemsCaptor.capture())
        val itemsSentToReverseRemove = itemsCaptor.getValue.asInstanceOf[Set[SkuItem]]

        itemsSentToReverseRemove shouldBe Set(SkuItem("3"), SkuItem("4"))
      }
    }

    it("should attempt to reverse segment updates when a segment update fails") {
      val promotion = Promotion(
        Descriptor(UUID.randomUUID(), "E25137", "E25137 desc", "50.0% OFF 1 IN E25137_one IF 1 IN E25137_zero"),
        LineItemPromotion(OffSiteRedemption(SupplierProvidedReward), Qualification(MatchOperator(EQ, Count(1), IN(NamedProductSegmentDescriptor(SelfStyledNamespace("E25233_zero"), GroupNamespace("ape"), "E25233_zero", None))))),
        DateRange(DateTime.parse("2020-01-27"), DateTime.parse("2020-01-28")))

      val existingSegment = productSegment("segmentName", "1", "2")
      val updatedSegment = productSegment("segmentName", "2", "3", "4")
      val id = Identification.namespaceForSegment(existingSegment.descriptor).getOrElse("").toString

      val segments = List(updatedSegment)

      when(mockPromotionReaderClient.retrievePromotions())
        .thenReturn(success(List())).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.getSegment(id))
        .thenReturn(success(existingSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.addSegmentItems(segments.head, Set(SkuItem("3"), SkuItem("4"))))
        .thenReturn(success(updatedSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.removeSegmentItems(segments.head, Set(SkuItem("1"))))
        .thenReturn(fail(StatusCodes.BadRequest)).thenThrow(unexpectedCall)

      // reverse actions
      when(mockPromotionWriterClient.removeSegmentItems(segments.head, Set(SkuItem("3"), SkuItem("4"))))
        .thenReturn(success(existingSegment)).thenThrow(unexpectedCall)
      when(mockPromotionWriterClient.addSegmentItems(segments.head, Set(SkuItem("1"))))
        .thenReturn(success(existingSegment)).thenThrow(unexpectedCall)

      val futureResult = promotionService.writePromotion(PromotionAndSegments(promotion, segments))
      whenReady(futureResult, timeout(6 seconds)) { result =>
        result shouldBe Unsuccessful("failed")
        val segmentCaptor = ArgumentCaptor.forClass(classOf[ProductSegment])
        val itemsCaptor = ArgumentCaptor.forClass(classOf[Set[SkuItem]])

        verify(mockPromotionWriterClient, times(2)).removeSegmentItems(segmentCaptor.capture(), itemsCaptor.capture())
        val itemsSentToRemoveFromSegment = itemsCaptor.getValue.asInstanceOf[Set[SkuItem]]
        itemsSentToRemoveFromSegment shouldBe Set(SkuItem("3"), SkuItem("4"))

        verify(mockPromotionWriterClient, times(2)).addSegmentItems(segmentCaptor.capture(), itemsCaptor.capture())
        val itemsSentToAddToSegment = itemsCaptor.getValue.asInstanceOf[Set[SkuItem]]
        itemsSentToAddToSegment shouldBe Set(SkuItem("1"))
      }
    }

  }

  private def productSegment(name: String, skus: String*) = {
    ProductSegment(NamedProductSegmentDescriptor(SelfStyledNamespace(name), GroupNamespace(""), "subject", None),
      skus.map(SkuItem).toSet)
  }
}
