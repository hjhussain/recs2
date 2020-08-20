package uk.co.argos.pap.promotion.ingest.service

import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Segment}
import uk.co.argos.pap.promotions.model.SegmentModels._

import scala.collection.mutable

trait InMemorySegmentService extends SegmentService {

  private lazy val segmentMap: mutable.Map[String, Segment] =
    new mutable.ListMap[String, Segment]

  def findBy(descriptor: SegmentDescriptor): Option[Segment] =
    descriptor match {
      case s: InlinedProductSegmentDescriptor => Some(ProductSegment(s, s.subject))
      case _ => segmentMap.get(namespaceFor(descriptor))
    }

  override def create(segment: Segment): SegmentDescriptor =
    segment.descriptor match {
      case d: SegmentDescriptor => segmentMap.put(namespaceFor(d), segment)
        segment.descriptor match {
          case s: SegmentDescriptor => s
        }
    }

}
