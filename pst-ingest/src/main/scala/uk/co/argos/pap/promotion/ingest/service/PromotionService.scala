package uk.co.argos.pap.promotion.ingest.service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import com.typesafe.scalalogging.LazyLogging
import uk.co.argos.pap.promotion.ingest.http.{PromotionReaderClient, PromotionWriterClient}
import uk.co.argos.pap.promotion.ingest.pst.PromotionAndSegments
import uk.co.argos.pap.promotions.domain.Identification
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Promotion, Segment}
import uk.co.argos.pap.promotions.model.RewardModels.SkuItem
import uk.co.argos.pap.promotions.model.SegmentModels._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


object PromotionService {
  def apply(promotionWriterClient: PromotionWriterClient,
            promotionReaderClient: PromotionReaderClient)
           (implicit ec: ExecutionContext, actorSystem: ActorSystem): PromotionService =
    new PromotionService(promotionWriterClient, promotionReaderClient)
}

sealed trait PromotionWriteAction {
  def reverse(): PromotionWriteAction
}

case object NoOp extends PromotionWriteAction {
  override def reverse(): PromotionWriteAction = {
    throw new UnsupportedOperationException("NoOp cannot not be reversed")
  }
}

case class RemoveSegmentItems(segment: ProductSegment,
                              items: Set[SkuItem]) extends PromotionWriteAction {
  override def reverse(): PromotionWriteAction =
    AddSegmentItems(segment, items)
}

case class AddSegmentItems(segment: ProductSegment,
                           items: Set[SkuItem]) extends PromotionWriteAction {
  override def reverse(): PromotionWriteAction = RemoveSegmentItems(segment, items)

}

case class CreateSegment(segment: ProductSegment) extends PromotionWriteAction {
  override def reverse(): PromotionWriteAction = NoOp
}


sealed trait OperationStatus {
  def message: String
}

final case class Successful(message: String = "success") extends OperationStatus

final case class Unsuccessful(message: String) extends OperationStatus

class PromotionService(promotionWriterClient: PromotionWriterClient,
                       promotionReaderClient: PromotionReaderClient)
                      (implicit ec: ExecutionContext, actorSystem: ActorSystem) extends LazyLogging {

  def retrieveExistingPromotions(): Future[List[Promotion]] = {
    promotionReaderClient.retrievePromotions().map {
      case Right(promotions) => promotions
      case ariError => throw new Exception(s"unable to retrieve promotions $ariError")
    }
  }

  def getSegment(descriptor: SegmentDescriptor): Future[Option[ProductSegment]] = {
    val result = Identification.namespaceForSegment(descriptor)
      .map(uuid => promotionWriterClient.getSegment(uuid.toString)
        .flatMap {
          case Right(existingSegment) => Future.successful(Some(existingSegment))
          case Left(apiError) if apiError.statusCode == StatusCodes.NotFound =>
            Future.successful(None)
          case Left(apiError) => Future.failed(new Exception(apiError.message))
        })

    result match {
      case Right(segment) => segment
      case Left(t) => Future.failed(t)
    }
  }

  def genSegmentUpdates(segment: Segment): Future[Seq[PromotionWriteAction]] = {
    segment match {
      case ps: ProductSegment =>
        getSegment(ps.descriptor).map {
          case Some(existingSegment) =>
            val toAdd = ps.items.diff(existingSegment.items)
            val toRemove = existingSegment.items.diff(ps.items)
            val addAction = if (toAdd.nonEmpty) List(AddSegmentItems(ps, toAdd)) else Nil
            val removeAction = if(toRemove.nonEmpty) List(RemoveSegmentItems(ps, toRemove)) else Nil
            addAction ++ removeAction
          case None =>
            List(CreateSegment(ps))
        }
    }
  }

  def executeAction(action: PromotionWriteAction): Future[Either[PromotionWriteAction, PromotionWriteAction]] = {
    val result = action match {
      case a: AddSegmentItems =>
        promotionWriterClient.addSegmentItems(a.segment, a.items)
      case a: RemoveSegmentItems =>
        promotionWriterClient.removeSegmentItems(a.segment, a.items)
      case a: CreateSegment =>
        promotionWriterClient.createSegment(a.segment)
      case NoOp =>
        Future.successful(Right("ok"))
    }
    result.map {
      case Right(_) => Right(action.reverse())
      case Left(err) =>
        logger.error(s"failed to: $action  error=$err")
        Left(action.reverse())
    }.recover { case _ => Left(action.reverse()) }
  }

  def writePromotion(promotionAndSegments: PromotionAndSegments): Future[OperationStatus] = {

    logger.debug(s"writePromotion: $promotionAndSegments")
    val eventualActions = Future.sequence(
      promotionAndSegments.segments.filterNot(_.descriptor match {
        case _: InlinedProductSegmentDescriptor => true
        case _ => false
      }).map(s => genSegmentUpdates(s))
    )

    eventualActions.flatMap(actions => {

      val eventualSegmentUpdates: Future[List[Either[PromotionWriteAction, PromotionWriteAction]]] =
        Future.sequence(actions.flatMap(_.map(executeAction)))

      eventualSegmentUpdates.flatMap(updates => {
        val failedReverseActions = updates.collect { case Left(a) => a }
        val succeededReverseActions = updates.collect { case Right(a) => a }
        if (failedReverseActions.isEmpty)
          promotionWriterClient.createPromotion(promotionAndSegments.promotion).map {
            case Right(r) => Right(r)
            case Left(err) =>
              logger.error(s"failed to create promotion $err")
              Left(succeededReverseActions)
          }.recover { case _ => Left(succeededReverseActions) }
        else
          Future.successful(Left(failedReverseActions ++ succeededReverseActions))
      }).map {
        case Right(r) => Successful(r)
        case Left(reverseActions) =>
          reverseActions.foreach(a => Try(executeAction(a)))
          Unsuccessful("failed")
      }.recover {case t => Unsuccessful(t.getMessage)}
    })
  }

}
