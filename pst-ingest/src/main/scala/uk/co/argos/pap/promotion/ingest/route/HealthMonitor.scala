package uk.co.argos.pap.promotion.ingest.route

import akka.util.Timeout
import uk.co.argos.pap.platform.directive.PapDirectives
import uk.co.argos.pap.platform.service.message.CommonMessages

import scala.concurrent.{ExecutionContext, Future}

final class HealthMonitor()(implicit executionContext: ExecutionContext, timeout: Timeout) extends PapDirectives {
  def health: Future[CommonMessages.Result] =
    Future.successful(CommonMessages.Result(status = true, "We're OK!"))
}