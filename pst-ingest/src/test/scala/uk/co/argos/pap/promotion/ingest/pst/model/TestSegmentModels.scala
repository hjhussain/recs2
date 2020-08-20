package uk.co.argos.pap.promotion.ingest.pst.model

import java.util.UUID

import org.joda.time.{DateTime, Instant}
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Segment}
import uk.co.argos.pap.promotions.model.RewardModels._
import uk.co.argos.pap.promotions.model.SegmentModels._
import uk.co.argos.pap.promotions.segex.SegmentExpressionParser

import uk.co.argos.pap.promotions.model.SegmentModels

import scala.language.implicitConversions

object TestSegmentModels {

  object Skus {

    val xboxOneX = SkuItem("7423611")
    val xbox = SkuItem("8637712")

    val redDeadXbox = SkuItem("8198983")
    val cod4Xbox = SkuItem("8108399")
    val fifa19Xbox = SkuItem("8636500")
    val deadRising2Xbox = SkuItem("4023870")
    val fallout4Xbox = SkuItem("4489263")
    val raymanLegendsXbox = SkuItem("5170124")


    val ps4Pro = SkuItem("6185936")
    val ps4 = SkuItem("5639847")

    val moveControllerTwinPack = SkuItem("8375665")

    val redDeadPs4 = SkuItem("8209322")
    val cod4Ps4 = SkuItem("8108375")
    val fifa19Ps4 = SkuItem("8639215")
    val fallout4Ps4 = SkuItem("4506122")
    val portalKnights4Ps4 = SkuItem("6849124")

  }

  object Users {
    val theQueen = User(UUID.randomUUID(), "ElizabethAlexandraMary@blighty.co.uk", "SW1A 1AA")
    val kalin = User(UUID.randomUUID(), "batman@argos.co.uk", "B4T C4VE")
  }

  object Products {

    import Skus._

    val now: Instant = DateTime.now().toInstant

    def productSegmentDescriptor(name: String, expression: String): ProductSegmentDescriptor = {
      NamedProductSegmentDescriptor(
        SelfStyledNamespace(name),
        GroupNamespace(SegmentExpressionParser.apeNamespace),
        name
      )
    }

    val threeItemBasket = ProductSegment(
      BasketSegmentDescriptor(),
      Set(ps4Pro, ps4, cod4Xbox)
    )

    def productSegment(productSegmentDescriptor: ProductSegmentDescriptor, items: Set[SkuItem]): ProductSegment = {
      ProductSegment(productSegmentDescriptor, items, now, now)
    }

    val xboxGames: ProductSegment = productSegment(
      productSegmentDescriptor("xbox_games", "xbox games"),
      Set(redDeadXbox, cod4Xbox, fifa19Xbox, deadRising2Xbox, fallout4Xbox, raymanLegendsXbox)
    )

    val ps4Games: ProductSegment = productSegment(
      productSegmentDescriptor("psfour_games", "PS4 games"),
      Set(redDeadPs4, cod4Ps4, fifa19Ps4, fallout4Ps4, portalKnights4Ps4)
    )

    val ps4VRControllers: ProductSegment = productSegment(
      productSegmentDescriptor("psfour_vr_controllers", "PS4 VR controllers"),
      Set(moveControllerTwinPack)
    )

    val Ps4Segment: ProductSegment = productSegment(
      productSegmentDescriptor("ps4", "a segment for a Ps4s"),
      Set(ps4Pro, ps4)
    )

    val xboxSegment: ProductSegment = productSegment(
      productSegmentDescriptor("xbox", "a segment for xboxs"),
      Set(xboxOneX, xbox)
    )

    val gamesSegment = ProductSegment(
      productSegmentDescriptor("all_games", "All games"),
      Set(redDeadPs4, cod4Ps4, fifa19Ps4, fallout4Ps4, portalKnights4Ps4, redDeadXbox, cod4Xbox, fifa19Xbox, deadRising2Xbox, fallout4Xbox, raymanLegendsXbox)
    )
  }

  object ExposedSegments {

    import TestSegmentModels.Products._

    implicit def toReference(segment: Segment): SegmentDescriptor = segment match {
      case ProductSegment(d, _, _, _) => d
    }

    case class SegmentWithItems(
      segmentOperator: SegmentOperator,
      products: Set[SkuItem]
    )

    val xboxGamesSegment = SegmentWithItems(IN(xboxGames), xboxGames.items)
    val inPS4Games = SegmentWithItems(IN(ps4Games), ps4Games.items)

    def mWithN(segment: ProductSegment*)(m: Int, n: Int): SegmentWithItems = {
      val segmentOperator = segment.toList match {
        case x :: Nil => IN(x)
        case x :: t :: Nil => OR(IN(x), IN(t))
        case x :: y :: t => t.foldLeft(OR(IN(x), IN(y))){
          case (acc, v) => OR(acc, IN(v))
        }
      }

      val items = segment.foldLeft(Set.empty[SkuItem]){
        (acc, v) => acc ++ v.items
      }

      SegmentWithItems(
        AND(
          SegmentModels.MatchOperator(EQ, Count(m), segmentOperator),
          With(SegmentModels.MatchOperator(EQ, Count(n), segmentOperator))
        ),
        items
      )
    }

    def gteMoney(segment: ProductSegment)(money: BigDecimal) = SegmentWithItems(
      SegmentModels.MatchOperator(GTE, Value(money), IN(segment)),
      segment.items
    )

    def gtMoney(segment: ProductSegment)(money: BigDecimal) = SegmentWithItems(
      SegmentModels.MatchOperator(GT, Value(money), IN(segment)),
      segment.items
    )

    def gtBasket(segment: ProductSegment)(money: BigDecimal) = SegmentWithItems(
      SegmentModels.MatchOperator(GT, Value(money), IN(BasketSegmentDescriptor())),
      segment.items
    )

    def ltBasket(segment: ProductSegment)(money: BigDecimal) = SegmentWithItems(
      SegmentModels.MatchOperator(LT, Value(money), IN(BasketSegmentDescriptor())),
      segment.items
    )

    def gtePs4Games: BigDecimal => SegmentWithItems = gteMoney(ps4Games)
    def gtPs4Games: BigDecimal => SegmentWithItems = gtMoney(ps4Games)
    def gtInBasketHavingPs4Games: BigDecimal => SegmentWithItems = gtBasket(ps4Games)
    def ltInBasketHavingPs4Games: BigDecimal => SegmentWithItems = ltBasket(ps4Games)

    def mWithNPs4Games(m: Int, n: Int): SegmentWithItems = mWithN(ps4Games)(m, n)
    def mWithNInGames(m: Int, n: Int): SegmentWithItems = mWithN(ps4Games)(m, n)

    val over30PoundsInXboxGamesSegment = SegmentWithItems(MatchOperator(GT, Value(30.00), IN(xboxGames)), xboxGames.items)

    val matchOneInXboxGamesSegment = SegmentWithItems(MatchOperator(EQ, Count(1), IN(xboxGames)), xboxGames.items)

    val inXboxGamesWithOneInXboxQualifier = SegmentWithItems(
      AND(xboxGamesSegment.segmentOperator, Qualification(MatchOperator(EQ, Count(1), IN(xboxSegment)))),
      xboxGamesSegment.products ++ xboxSegment.items
    )

    val inXboxGamesWithOneInXbox = SegmentWithItems(
      AND(xboxGamesSegment.segmentOperator, MatchOperator(EQ, Count(1), IN(xboxSegment))),
      xboxGamesSegment.products ++ xboxSegment.items
    )

    val gamesSegment: (OR, Set[SkuItem]) =  (
      OR(
        xboxGamesSegment.segmentOperator,
        inPS4Games.segmentOperator
      ),
      xboxGamesSegment.products ++ ps4Games.items
    )

    val inGames = SegmentWithItems(
      gamesSegment._1,
      gamesSegment._2
    )

    val inXboxGamesOrPS4Games = SegmentWithItems(
      OR(IN(xboxGames), IN(ps4Games)),
      xboxGames.items ++ ps4Games.items
    )
  }
}
