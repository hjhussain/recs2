package uk.co.argos.pap.promotion.ingest.pst

import java.util.UUID

import cats.data.Validated.{Valid, invalidNel}
import cats.data.ValidatedNel
import co.uk.argos.pap.wcs.data.model.PST
import co.uk.argos.pap.wcs.data.model.PST.{BasketRewardRules, NoDiscount, ProductSet}
import org.joda.time.DateTime
import uk.co.argos.pap.promotion.ingest.service.SegmentService
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Promotion, Segment}
import uk.co.argos.pap.promotions.model.PromotionModels._
import uk.co.argos.pap.promotions.model.RewardModels.{All, Limit, Money, Percent, SkuItem, SupplierProvidedReward}
import uk.co.argos.pap.promotions.model.{RewardModels, SegmentModels}
import uk.co.argos.pap.promotions.model.SegmentModels._
import uk.co.argos.pap.promotions.parsing.ExpressionUtils
import uk.co.argos.pap.promotions.segex.SegmentExpressionParser

import scala.language.{implicitConversions, postfixOps}

case class PromotionAndSegments(promotion: Promotion, segments: List[Segment])

object Translator {
  val one: RewardModels.RewardCount = Limit(1)
  val all: RewardModels.All.type = All

  def translate(segmentService: SegmentService)(
    pstPromotion: PST.Promotion): ValidatedNel[String, PromotionAndSegments] = {


    def toWord(int: Int): String = int match {
      case 0 => "zero"
      case 1 => "one"
      case 2 => "two"
      case 3 => "three"
    }

    def toSegmentOperation(tuple: (ProductSet, ProductSegment, SegmentDescriptor)): SegmentOperator = {
      val (productSet, _, segmentDescriptor) = tuple
      if (productSet.lowerBuyQuantity.getOrElse(0) > 0)
        SegmentModels.MatchOperator(
          EQ,
          Count(productSet.lowerBuyQuantity.getOrElse(0)),
          IN(segmentDescriptor)
        )
      else IN(segmentDescriptor)
    }

    def anded(ps: List[(ProductSet, ProductSegment, SegmentDescriptor)]): SegmentOperator = ps match {
      case h :: Nil => toSegmentOperation(h)
      case h :: n :: Nil => AND(toSegmentOperation(h), toSegmentOperation(n))
      case h :: t => AND(
        toSegmentOperation(h),
        anded(t)
      )
    }

    def hasBasketReward: Boolean = {
      pstPromotion.basketRewardRules.exists(_.rewardOption.nonEmpty)
    }

    def descriptor(promotionExpression: PromotionExpression) = Descriptor(
      UUID.randomUUID(),
      pstPromotion.overview.id,
      pstPromotion.overview.promotionLongDescription,
      ExpressionUtils.expressionForPromotionExpression(promotionExpression)
    )

    def resolveOperator(productSet: ProductSet,
                        productSegment: ProductSegment,
                        segmentDescriptor: SegmentDescriptor): SegmentOperator = {
      val getQuantity = productSet.getQuantity.getOrElse(0)
      val lowerBuyQuantity = productSet.lowerBuyQuantity.getOrElse(0)

      def segOp(q: Int): SegmentOperator = {
        if (q > 0)
          SegmentModels.MatchOperator(
            EQ,
            Count(q),
            IN(segmentDescriptor)
          ) else IN(segmentDescriptor)
      }

      if (productSet.rewardOption != NoDiscount && getQuantity != 0 && lowerBuyQuantity > getQuantity) {
        AND(segOp(getQuantity), With(segOp(lowerBuyQuantity - getQuantity)))
      } else if ((productSet.rewardOption == NoDiscount || (getQuantity == 0 && !hasBasketReward)) && lowerBuyQuantity > 0) {
        Qualification(segOp(lowerBuyQuantity))
      } else {
        anded(List((productSet, productSegment, segmentDescriptor)))
      }
    }

    def resolveBasketPromotionType: PartialFunction[BasketRewardRules, Option[PromotionType]] = {
      case x => x.rewardOption.flatMap(resolveRewardOption(_, x.rewardValue))
    }

    def resolvePromotionType: PartialFunction[ProductSet, Option[PromotionType]] = {
      case x =>
        if (x.getQuantity.contains(0) & hasBasketReward)
          resolveBasketPromotionType(pstPromotion.basketRewardRules.get) else
          resolveRewardOption(x.rewardOption, x.rewardValue)
    }

    def resolveRewardOption: PartialFunction[(PST.RewardOption, Option[Double]), Option[PromotionType]] = {
      case (PST.NoDiscount, _) => Some(OffSiteRedemption(SupplierProvidedReward))
      case (PST.SetPrice, x) => x.map(m => SetPrice(Money(m), all))
      case (PST.MoneyOff, x) => x.map(m => Discount(Money(m), all))
      case (PST.PercentageOff, x) => x.map(m => Discount(Percent(m), all))
    }


    def combineOperators(a: SegmentOperator, b: SegmentOperator): SegmentOperator = {
      (a, b) match {
        case (AND(lr, Qualification(segmentOperator)), Qualification(secondSegmentOperator)) =>
          AND(lr, Qualification(AND(segmentOperator, secondSegmentOperator)))
        case (Qualification(segmentOperator), _) =>
          Qualification(
            AND(
              segmentOperator,
              b match {
                case Qualification(rs) => rs
                case v => v
              }
            )
          )
        case (z, x) =>
          AND(z, x)
      }
    }

    def combineWithAND(segmentOperator: SegmentOperator, andPromotion: ANDPromotion): ANDPromotion = {
      def anded(promotionExpression: PromotionExpression, lineItemPromotion: LineItemPromotion): ANDPromotion = {
        ANDPromotion(
          promotionExpression,
          LineItemPromotion(
            lineItemPromotion.promotionType,
            combineOperators(lineItemPromotion.segmentOperator, segmentOperator)
          )
        )
      }

      andPromotion match {
        case ANDPromotion(s@LineItemPromotion(_, _), r) => anded(r, s)
        case ANDPromotion(l, s@LineItemPromotion(_, _)) => anded(l, s)
        case a@ANDPromotion(x: ANDPromotion, _) => a.copy(left = combineWithAND(segmentOperator, x))
      }
    }


    val pSets: List[(ProductSet, ProductSegment)] = pstPromotion.productSets.zipWithIndex.map {
      case (ps, i) =>
        val now = DateTime.now().toInstant
        ps -> {
          val skuItems = ps.products.map(pc => SkuItem(pc.sku)).toSet
          val descriptor = if (ps.products.size <= 10) {
            InlinedProductSegmentDescriptor(skuItems)
          } else {
            val aka = s"${pstPromotion.overview.id}_${toWord(i)}"

            NamedProductSegmentDescriptor(
              SelfStyledNamespace(aka),
              GroupNamespace(SegmentExpressionParser.apeNamespace),
              aka
            )
          }
          ProductSegment(
            descriptor,
            ps.products.map(pc => SkuItem(pc.sku)).toSet,
            now,
            now)
        }
    }

    val withSegmentDescriptors: List[(ProductSet, ProductSegment, SegmentDescriptor)] =
      pSets.map(p => (p._1, p._2, segmentService.create(p._2)))

    val promotionExpressions: List[ValidatedNel[String, PromotionExpression]] = withSegmentDescriptors.map {
      case (ps, s, sd) =>
        val operator = resolveOperator(ps, s, sd)
        val promo = resolvePromotionType(ps)
        promo.map(
          LineItemPromotion(_, operator)
        ).map(
          Valid(_)
        ).getOrElse(invalidNel("invalid promotion type, expected some but got none"))
    }
    val combinedPromotionExpressions: ValidatedNel[String, PromotionExpression] = promotionExpressions reduce {
      (x, y) =>
        x.product(y) andThen {
          case (l: LineItemPromotion, r: LineItemPromotion) => l.promotionType -> r.promotionType match {
            case (_: OffSiteRedemption, _: OffSiteRedemption) => Valid(
              LineItemPromotion(l.promotionType, combineOperators(l.segmentOperator, r.segmentOperator))
            )
            case (_: OffSiteRedemption, p) => Valid(
              LineItemPromotion(p, combineOperators(r.segmentOperator, l.segmentOperator))
            )
            case (p, _: OffSiteRedemption) => Valid(
              LineItemPromotion(p, combineOperators(l.segmentOperator, r.segmentOperator))
            )
            case (_, _) => Valid(
              ANDPromotion(l, r)
            )
          }
          case (l: LineItemPromotion, r: ANDPromotion) => l.promotionType match {
            case _: OffSiteRedemption => Valid(combineWithAND(l.segmentOperator, r))
            case _ => Valid(ANDPromotion(l, r))
          }
          case (l: ANDPromotion, r: LineItemPromotion) => r.promotionType match {
            case _: OffSiteRedemption => Valid(combineWithAND(r.segmentOperator, l))
            case _ => Valid(ANDPromotion(l, r))
          }
        }
    }

    val promotion = combinedPromotionExpressions map {
      case s@LineItemPromotion(promotionType, segmentOperator) => promotionType match {
        case _: OffSiteRedemption => pstPromotion.basketRewardRules.flatMap(resolveBasketPromotionType) map { p =>
          LineItemPromotion(p, segmentOperator)
        } getOrElse s
        case _ => s
      }
      case x => x
    } map {
      p =>
        Promotion(descriptor(p), p,
          DateRange(pstPromotion.overview.startDate, pstPromotion.overview.endDate))
    }

    promotion.map(
      p => PromotionAndSegments(p, withSegmentDescriptors.map(_._2).filterNot {
        _.descriptor match {
          case i: InlinedProductSegmentDescriptor => true
          case _ => false
        }
      })
    )
  }
}