package uk.co.argos.pap.promotion.ingest.pst

import java.util.UUID

import cats.Id
import cats.data.Validated.Valid
import cats.data.ValidatedNel
import co.uk.argos.pap.wcs.data.client.{PromotionsClient, PromotionsLoader}
import co.uk.argos.pap.wcs.data.config.Configurations.FileDataConfig
import co.uk.argos.pap.wcs.data.load.{CachingLoader, DataLoader}
import co.uk.argos.pap.wcs.data.model.PST
import co.uk.argos.pap.wcs.data.model.PST._
import co.uk.argos.pap.wcs.data.parse.PromotionParser
import org.apache.commons.io.IOUtils
import org.joda.time.DateTime
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import uk.co.argos.pap.promotion.ingest.pst.model.TestSegmentModels.Products._
import uk.co.argos.pap.promotion.ingest.service.InMemorySegmentService
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Promotion}
import uk.co.argos.pap.promotions.model.PromotionModels._
import uk.co.argos.pap.promotions.model.RewardModels.{SkuItem, SupplierProvidedReward}
import uk.co.argos.pap.promotions.model.SegmentModels._
import uk.co.argos.pap.promotions.segex.PromoExParser.PromoExResult
import uk.co.argos.pap.promotions.segex.SegmentExpressionParser.SegExResult
import uk.co.argos.pap.promotions.segex.{LineItemPromotionParser, PromoExParser, PromoSegExParser, SegmentValueValidator}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class TranslatorSpec extends FunSpec
  with Matchers
  with MockitoSugar
  with ScalaFutures
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  private def promotionsClient(resourcePath: String = "/promotions/PromotionSet.xml"): PromotionsClient = new PromotionsClient {
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

  private val testPromotionName = "TEST01"

  private def productSet(lowerBuy: Int, getQuantity: Int, value: Option[Double],
                         rewardOption: RewardOption, productSegment: ProductSegment): ProductSet = {
    ProductSet(
      0,
      Option(lowerBuy),
      None,
      Option(getQuantity),
      rewardOption,
      value,
      None, None, None,
      byAll = false,
      None,
      productSegment.items.map(g => ProductCode(g.sku)).toList
    )
  }

  private def pstPromotion(productSets: List[ProductSet],
                           basketRewardRules: Option[BasketRewardRules] = None): PST.Promotion = {
    val overview = PromotionOverview(
      onlinePromotionDisplayOption = "",
      id = testPromotionName,
      startDate = DateTime.now(),
      endDate = DateTime.now(),
      shortReceipt = None,
      fulfilmentMethods = None,
      publications = Nil,
      promotionType = "WOWOW",
      promotionShortDescription = "a test promotion",
      promotionLongDescription = testPromotionName,
      badgeRef = None,
      badgeText = None
    )

    PST.Promotion(
      Addition,
      overview,
      productSets,
      basketRewardRules)
  }

  private def assertEqual(actual: ValidatedNel[String, Promotion], expected: PromoExResult[PromotionExpression], expression: String): Assertion = {
    actual product expected map {
      case (p: Promotion, s: PromotionExpression) =>
        p.promotionExpression shouldEqual s
        p.descriptor.expression shouldEqual expression
      case x => fail(s"expected a promotion and SinglePromotion expression but got: $x")
    } getOrElse fail(s"expected expression, $expected, to be equal to translation but instead got: $actual")
  }

  private val segExParser = new PromoSegExParser[Id] with SegmentValueValidator[Id] {
    def validateSku(sku: String): Id[SegExResult[String]] = Valid(sku)
    def validateCategory(category: Int): Id[SegExResult[Int]] = Valid(category)
    def validateBrand(brand: String): Id[SegExResult[String]] = Valid(brand)
  }

  private val promoExParser: String => PromoExResult[PromotionExpression] =
    PromoExParser.expressionParser[Id, LineItemPromotion](new LineItemPromotionParser[Id](segExParser))

  def inlined(productSegment: ProductSegment): String = {
    s"Product(${productSegment.items.map(_.sku).mkString(" ")})"
  }


  describe("PST Tests") {
    it("Translated PSTs correctly") {
      val futurePsts = PromotionIngestionService.loadPstPromotions(promotionsClient())

      whenReady(futurePsts, timeout(6 seconds)) { psts =>
        val translator = Translator.translate(new InMemorySegmentService {}) _

        val promotion = translator(psts.head) match {
          case Valid(p: PromotionAndSegments) => p.promotion
        }
        val expected = Promotion(
          Descriptor(UUID.randomUUID(), "E25137", "E25137 desc", "GET * 50.0% OFF 1 IN E25137_one IF 1 IN E25137_zero"),
          LineItemPromotion(
            OffSiteRedemption(SupplierProvidedReward),
            Qualification(
              MatchOperator(
                EQ,
                Count(1),
                IN(
                  InlinedProductSegmentDescriptor(
                    Set(SkuItem("4591663"), SkuItem("4825054"),SkuItem("4961341"), SkuItem("8058298"))
                  )
                )
              )
            )
          ),
          DateRange(DateTime.parse("2020-01-27"), DateTime.parse("2020-01-28")))

        promotion.promotionExpression shouldBe expected.promotionExpression
        promotion.dateRange shouldBe expected.dateRange
      }
    }

    it("should translate £10 set price offer on ps4 games") {
      val pst = pstPromotion(List(
        productSet(lowerBuy = 1, getQuantity = 1, value = Some(10), PST.SetPrice, ps4Games)
      ))
      val expression = s"GET * £10.0 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)

      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)
      assertEqual(promotion, expected, expression)
    }

    it("should translate £10 OFF on ps4 games") {
      val pst = pstPromotion(List(
        productSet(lowerBuy = 1, getQuantity = 1, value = Some(10), PST.MoneyOff, ps4Games)
      ))
      val expression = s"GET * £10.0 OFF 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)

      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)
      assertEqual(promotion, expected, expression)
    }

    it("should translate 10% OFF on ps4 games") {
      val pst = pstPromotion(List(
        productSet(lowerBuy = 1, getQuantity = 1, value = Some(10), PST.PercentageOff, ps4Games)
      ))
      val expression = s"GET * 10.0% OFF 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }

    it("should translate a 3 for 2 on on ps4 games") {
      val pst = pstPromotion(List(
        productSet(lowerBuy = 3, getQuantity = 1, value = Some(0), PST.SetPrice, ps4Games)
      ))
      val expression = s"GET * £0.0 1 IN ${inlined(ps4Games)} WITH 2 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }

    it("should translate a bundle offer of 15% off a game with a playstation") {
      val pst = pstPromotion(List(
        productSet(lowerBuy = 1, getQuantity = 1, value = Some(15), PST.PercentageOff, ps4Games),
        productSet(lowerBuy = 1, getQuantity = 0, value = None, PST.NoDiscount, ps4Games)
      ))
      val expression = s"GET * 15.0% OFF 1 IN ${inlined(ps4Games)} IF 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }

    it("should translate a set price bundle of items in one product set when the reward is in basket reward rules") {
      val pst = pstPromotion(List(
        productSet(lowerBuy = 3, getQuantity = 0, value = Some(100), PST.PercentageOff, ps4Games),
      ),
        Some(
          BasketRewardRules(
            None,
            Some(PST.SetPrice),
            Some(50)
          )
        ))
      val expression = s"GET * £50.0 3 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }

    it("should translate a ps4 mega bundle with set price controller and a free game when buying a ps4") {
      val pst = pstPromotion(
        List(
          productSet(lowerBuy = 1, getQuantity = 1, value = Some(50), PST.SetPrice, ps4VRControllers),
          productSet(lowerBuy = 1, getQuantity = 0, value = None, PST.NoDiscount, Ps4Segment),
          productSet(lowerBuy = 1, getQuantity = 1, value = Some(100), PST.PercentageOff, ps4Games)
        ))
      val expression = s"GET * £50.0 1 IN ${inlined(ps4VRControllers)} IF 1 IN ${inlined(Ps4Segment)} AND GET * 100.0% OFF 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }

    it("should translate more than one bundle that is marked as no discount") {
      val pst = pstPromotion(
        List(
          productSet(lowerBuy = 1, getQuantity = 1, value = None, PST.NoDiscount, Ps4Segment),
          productSet(lowerBuy = 1, getQuantity = 1, value = Some(54.99), PST.SetPrice, ps4VRControllers),
          productSet(lowerBuy = 1, getQuantity = 1, value = None, PST.NoDiscount, ps4Games)
        ))

      val expression = s"GET * £54.99 1 IN ${inlined(ps4VRControllers)} IF 1 IN ${inlined(Ps4Segment)} AND 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }

    it("should translate more than one item is in the reward segment") {
      val pst = pstPromotion(
        List(
          productSet(lowerBuy = 1, getQuantity = 1, value = None, PST.NoDiscount, Ps4Segment),
          productSet(lowerBuy = 1, getQuantity = 1, value = Some(0), PST.SetPrice, ps4VRControllers),
          productSet(lowerBuy = 1, getQuantity = 1, value = Some(0), PST.SetPrice, xboxGames),
          productSet(lowerBuy = 1, getQuantity = 1, value = Some(30.00), PST.SetPrice, ps4VRControllers),
        ))

      val expression = s"GET * £30.00 1 IN ${inlined(ps4VRControllers)} IF 1 IN ${inlined(Ps4Segment)} AND 1 IN ${inlined(ps4Games)}"
      val expected = promoExParser(expression)
      val promotion = Translator.translate(new InMemorySegmentService {})(pst).map(_.promotion)

      assertEqual(promotion, expected, expression)
    }
  }

}
