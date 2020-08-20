package uk.co.argos.pap.promotion.ingest.model

import org.joda.time.Instant
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Promotion}
import uk.co.argos.pap.promotions.model.PromotionModels.DateRange
import uk.co.argos.pap.promotions.model.RewardModels.SkuItem
import uk.co.argos.pap.promotions.model.SegmentModels.{NamedProductSegmentDescriptor, ProductSegmentDescriptor}
import uk.co.argos.pap.promotions.model.StackingModels.{High, Low, Medium}
import uk.co.argos.pap.promotions.segex.SegmentExpressionParser


final case class PromotionRequestDescriptor(name: String,
                                            description: String,
                                            expression: String,
                                            created: Option[Instant] = None)

final case class PromotionRequestDto(descriptor: PromotionRequestDescriptor,
                                     dateRange: DateRange,
                                     stackingRule: Option[String],
                                     priority: Option[String],
                                     created: Option[Instant] = None)


final case class SkuItemDto(sku: String)
final case class SkuItemsDto(items: Set[SkuItemDto])

final case class ProductSegmentDescriptorRequestDto(name: String, description: String)

final case class ProductSegmentRequestDto(descriptor: ProductSegmentDescriptorRequestDto,
                                           items: Set[SkuItemDto])

final case class ProductSegmentDto(descriptor: ProductSegmentDescriptorDto, items: Set[SkuItemDto])
final case class ProductSegmentDescriptorDto(name: String, description: Option[String])


object Conversions {

  def promotionToRequest(p: Promotion): PromotionRequestDto =
    PromotionRequestDto(
      descriptor = PromotionRequestDescriptor(
        name = p.descriptor.name,
        description = p.descriptor.description,
        expression = p.descriptor.expression,
        created = Option(p.created)),
      dateRange = p.dateRange,
      stackingRule = Option(p.stackingRules.stackingRule.toString),
      priority = p.stackingRules.promotionPriority match {
        case Low.value => Some("low");
        case Medium.value => Some("medium")
        case High.value => Some("high")
        case _ => None
      },
      created = Option(p.created))

  def productSegmentToRequest(s: ProductSegment): ProductSegmentRequestDto = {

    def fromDescriptor(desc: ProductSegmentDescriptor): ProductSegmentDescriptorRequestDto =
      desc match {
        case NamedProductSegmentDescriptor(_, _, subject, description) =>
          ProductSegmentDescriptorRequestDto(subject, description.getOrElse(subject))
        case _ => ProductSegmentDescriptorRequestDto(desc.segmentTypeNamespace.value, desc.segmentTypeNamespace.value)
      }

    ProductSegmentRequestDto(
      fromDescriptor(s.descriptor),
      s.items.map(s => SkuItemDto(s.sku)))
  }

  def toProductSegment(ps: ProductSegmentDto): ProductSegment = {
    val instant = Instant.now()
    ProductSegment(NamedProductSegmentDescriptor(
      ps.descriptor.name,
      SegmentExpressionParser.apeNamespace,
      ps.descriptor.description),
      ps.items.map(s => SkuItem(s.sku)),
      instant,
      instant)
  }

  def skuItemsToRequest(skus: Set[SkuItem]): SkuItemsDto =
    SkuItemsDto(skus.map(s => SkuItemDto(s.sku)))

}