package uk.co.argos.pap.promotion.ingest.route

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging
import uk.co.argos.pap.platform.directive.PapDirectives
import uk.co.argos.pap.promotion.ingest.pst.IngestResult
import uk.co.argos.pap.promotion.ingest.util.Metrics
import uk.co.argos.pap.promotions.model.JournalPersistenceFormats

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

trait IngestRoute extends PapDirectives
  with LazyLogging
  with JournalPersistenceFormats {

  implicit val timeout: Timeout = 1 hour
  implicit val executionContext: ExecutionContext

  def ingestPstPromotions: () => Future[IngestResult]

  def ingestRoute: Route =
    pathPrefix("ingest") {
      path("promotions") {
        withoutRequestTimeout {
          post {
            onSuccess(ingestPstPromotions()) {
              result: IngestResult => {
                Metrics.incrementCounterMetric(name="pst-ingest-failures", times=result.failures)
                Metrics.incrementCounterMetric(name="pst-ingest-successes", times=result.successes)
                complete(StatusCodes.OK -> result)
              }
            }
          }
        }

      }
    }
}