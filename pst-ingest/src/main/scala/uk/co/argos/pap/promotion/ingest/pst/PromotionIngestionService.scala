package uk.co.argos.pap.promotion.ingest.pst

import java.util.concurrent.{Semaphore, TimeUnit}

import akka.stream.scaladsl.Source
import akka.stream.{Materializer, ThrottleMode}
import cats.data.Validated.Valid
import cats.data.ValidatedNel
import co.uk.argos.pap.wcs.data.client.PromotionsClient
import co.uk.argos.pap.wcs.data.model.PST
import co.uk.argos.pap.wcs.data.model.PST.NoDiscount
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import uk.co.argos.pap.promotion.ingest.factory.ThrottlingConfig
import uk.co.argos.pap.promotion.ingest.pst.PromotionIngestionService.{equivalent, loadPstPromotions, pstToPromotion}
import uk.co.argos.pap.promotion.ingest.service._
import uk.co.argos.pap.promotion.ingest.util.Metrics
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.Promotion

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{implicitConversions, postfixOps}

case class IngestResult(successes:Int, failures:Int)

object PromotionIngestionService {

  private val permitToIngest = new Semaphore(1)

  def apply(pstClient: PromotionsClient,
            promotionService: PromotionService,
            throttlingConfig: ThrottlingConfig)
           (implicit materializer: Materializer, ec: ExecutionContext): PromotionIngestionService =
    new PromotionIngestionService(pstClient, promotionService, throttlingConfig)

  def loadPstPromotions(pstClient: PromotionsClient,
                        forDate: DateTime = DateTime.now): Future[Seq[PST.Promotion]] =
    pstClient.promotionsFor(forDate)

  def removeUnsupported(psts: Seq[PST.Promotion]): Seq[PST.Promotion] = {
    def isNoDiscountReward(pst: PST.Promotion): Boolean =
      pst.productSets.forall(ps => ps.rewardOption == NoDiscount)

    psts.filterNot(pst => isNoDiscountReward(pst))
  }

  def equivalent(a: Promotion, b: Promotion): Boolean =
    a.dateRange == b.dateRange &&
      a.stackingRules == b.stackingRules &&
      a.valueOrder == b.valueOrder &&
      (a.descriptor.name == b.descriptor.name ||
        a.descriptor.description == b.descriptor.description ||
        a.descriptor.expression == b.descriptor.expression)


  def removeDuplicates(newPromotions: Seq[PromotionAndSegments],
                       existingPromotions: Seq[Promotion]): Seq[PromotionAndSegments] =
    if (existingPromotions.nonEmpty)
      newPromotions.filterNot(newPromo => existingPromotions
        .exists(existingPromo => equivalent(newPromo.promotion, existingPromo)))
    else
      newPromotions


  def pstToPromotion(segmentService: SegmentService): PST.Promotion => ValidatedNel[String, PromotionAndSegments] =
    Translator.translate(segmentService)
}

class PromotionIngestionService(pstClient: PromotionsClient,
                                promotionService: PromotionService,
                                throttlingConfig: ThrottlingConfig)
                               (implicit materializer: Materializer, ec: ExecutionContext)
  extends LazyLogging {

  def getNewPromotions(pstClient: PromotionsClient): Future[Seq[PromotionAndSegments]] = {

    val segmentService = new InMemorySegmentService {}
    loadPstPromotions(pstClient)
      .map(psts => PromotionIngestionService.removeUnsupported(psts)
        .map(pstToPromotion(segmentService)).collect { case Valid(p: PromotionAndSegments) => p })
  }

  def ingestPstPromotions(): Future[IngestResult] =
    promotionService.retrieveExistingPromotions()
      .flatMap(existingPromotions => {
        getNewPromotions(pstClient)
          .flatMap(newPromotions => {
            val now = DateTime.now
            val uniquePromotions = newPromotions.filterNot(p => existingPromotions
              .exists(existingPromo => equivalent(p.promotion, existingPromo)))
                .filterNot(_.promotion.dateRange.to.isBefore(now))
            logger.info(s"number of promotions loaded=${newPromotions.size}, number to write=${uniquePromotions.size}")
            writePromotionsWithThrottling(uniquePromotions, existingPromotions)
          })
      })

  def ingestPstPromotionsIfNoIngestionInProgress(): Future[IngestResult] = {
    Metrics.incrementCounterMetric("ingestionAttempts")
    if(PromotionIngestionService.permitToIngest.tryAcquire()) {
      try {
        ingestPstPromotions()
      } finally {
        PromotionIngestionService.permitToIngest.release()
      }
    } else {
      logger.info(s"ingestion is still in progress")
      Future.successful(IngestResult(0,0))
    }
  }

  def writePromotionsWithThrottling(newPromotions: Seq[PromotionAndSegments],
                                    existingPromotions: Seq[Promotion]): Future[IngestResult] =
    Source(newPromotions.toVector)
      .throttle(throttlingConfig.requests,
        Duration(throttlingConfig.requestsPerNSeconds, TimeUnit.SECONDS),
        throttlingConfig.maxBurst, ThrottleMode.Shaping)
      .runFoldAsync(IngestResult(0,0))((countSoFar, p) => promotionService.writePromotion(p).map {
        case Successful(_) =>
          logger.debug(s"created new promotion $p")
          countSoFar.copy(successes = countSoFar.successes + 1)
        case Unsuccessful(message) =>
          logger.error(s"failed to create promotion $p, $message")
          countSoFar.copy(failures = countSoFar.failures + 1)
      })
}