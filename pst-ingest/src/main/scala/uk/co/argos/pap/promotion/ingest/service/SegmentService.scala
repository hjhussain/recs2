package uk.co.argos.pap.promotion.ingest.service

import uk.co.argos.pap.promotions.model.JournalPersistenceModels.Segment
import uk.co.argos.pap.promotions.model.SegmentModels._

import scala.language.higherKinds

trait SegmentService extends SegmentOps {
  def create(segment: Segment): SegmentDescriptor
  def findBy(descriptor: SegmentDescriptor): Option[Segment]
}

trait SegmentOps {
  def namespaceFor(descriptor: SegmentDescriptor): String = {
    s"${descriptor.accessorNamespace.value}:${descriptor.segmentTypeNamespace.value}"
  }
}

object SegmentOps extends SegmentOps