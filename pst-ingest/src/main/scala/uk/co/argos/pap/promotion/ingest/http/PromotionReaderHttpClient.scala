package uk.co.argos.pap.promotion.ingest.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.stream.Materializer
import spray.json.RootJsonFormat
import uk.co.argos.pap.platform.http.ApiHttpClient.ApiError
import uk.co.argos.pap.promotion.ingest.factory.ServiceConfig
import uk.co.argos.pap.promotions.model.JournalPersistenceFormats
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.Promotion

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, reflectiveCalls}

object HttpResponseHandler {

  def handlerResponse[T, E](serviceName: String, response: HttpResponse)
                           (implicit successUnmarshaller: FromEntityUnmarshaller[T],
                            errorUnmarshaller: FromEntityUnmarshaller[E],
                            actorSystem: ActorSystem,
                            ec: ExecutionContext): Future[Either[ApiError[E], T]] =
    if (response.status.isSuccess())
      successUnmarshaller(response.entity).map(entity => Right(entity))
    else
      errorUnmarshaller(response.entity).map(entity => Left(ApiError(serviceName, response.status, Some(entity))))

}

trait PromotionReaderClient {
  def retrievePromotions(): Future[Either[ApiError[String], List[Promotion]]]
}
class PromotionReaderHttpClient(singleRequestReader: HttpRequest => Future[HttpResponse],
                                config: ServiceConfig)
                               (implicit materializer: Materializer, actorSystem: ActorSystem, ec: ExecutionContext)
    extends PromotionReaderClient
    with  JournalPersistenceFormats
    with SprayJsonSupport {

  implicit val promotionJsonFormat: RootJsonFormat[Promotion] = promotionFormat

  def retrievePromotions(): Future[Either[ApiError[String], List[Promotion]]] = {
    val uri = s"${config.host}/api/promotions"
    singleRequestReader(HttpRequest(uri = uri, method = HttpMethods.GET))
      .flatMap(response =>
        HttpResponseHandler.handlerResponse[List[Promotion], String](s"${config.serviceName}:$uri", response))
      .map {
        case Left(apiErr) if apiErr.statusCode.equals(StatusCodes.NotFound) => Right(List.empty[Promotion])
        case r => r
      }
  }
}

