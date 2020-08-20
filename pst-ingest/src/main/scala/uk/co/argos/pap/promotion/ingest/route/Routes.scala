package uk.co.argos.pap.promotion.ingest.route

import akka.event.Logging
import akka.http.scaladsl.server.Route

trait Routes extends IngestRoute {
  def route: Route = logRequestResult("debug", Logging.DebugLevel) {
    apiWithoutDefaultCache(ingestRoute)
  }
}
