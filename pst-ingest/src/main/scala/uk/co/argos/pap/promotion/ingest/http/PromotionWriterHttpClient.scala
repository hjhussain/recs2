package uk.co.argos.pap.promotion.ingest.http

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, RequestEntity}
import akka.stream.Materializer
import spray.json.RootJsonFormat
import uk.co.argos.pap.platform.http.ApiHttpClient.ApiResponse
import uk.co.argos.pap.promotion.ingest.factory.ServiceConfig
import uk.co.argos.pap.promotion.ingest.model._
import uk.co.argos.pap.promotions.domain.Identification
import uk.co.argos.pap.promotions.model.JournalPersistenceFormats
import uk.co.argos.pap.promotions.model.JournalPersistenceModels.{ProductSegment, Promotion, Segment}
import uk.co.argos.pap.promotions.model.PromotionModels.{BinaryPromotionOperator, LineItemPromotion, OrderLevelPromotion, PriceDropPromotion, PromotionExpression}
import uk.co.argos.pap.promotions.model.RewardModels.SkuItem

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, reflectiveCalls}


trait PromotionWriterClient {
  def createPromotion(promotion: Promotion): Future[ApiResponse[String, String]]

  def createSegment(segment: Segment): Future[ApiResponse[String, String]]

  def getSegment(id: String): Future[ApiResponse[ProductSegment, String]]

  def addSegmentItems(segment: Segment, items: Set[SkuItem]): Future[ApiResponse[ProductSegment, String]]

  def removeSegmentItems(segment: Segment, items: Set[SkuItem]): Future[ApiResponse[ProductSegment, String]]
}

class PromotionWriterHttpClient(singleRequestReader: HttpRequest => Future[HttpResponse],
                                config: ServiceConfig)
                               (implicit materializer: Materializer, actorSystem: ActorSystem, ec: ExecutionContext)
  extends PromotionWriterClient
    with JournalPersistenceFormats
    with SprayJsonSupport {
  implicit val promotionJsonFormat: RootJsonFormat[Promotion] = promotionFormat

  def createPromotion(promotion: Promotion): Future[ApiResponse[String, String]] = {
    @tailrec
    def fromPromotionExpressionType(promotionExpression: PromotionExpression): String = {
      promotionExpression match {
        case _: LineItemPromotion => "line"
        case _: OrderLevelPromotion => "order"
        case _: PriceDropPromotion => "price"
        case o: BinaryPromotionOperator => fromPromotionExpressionType(o.left)
      }
    }

    val req = Conversions.promotionToRequest(promotion)
    Marshal(req).to[RequestEntity].flatMap { e =>
      val urlPath = fromPromotionExpressionType(promotion.promotionExpression)
      val uri = s"${config.host}/api/promotion/$urlPath"
      singleRequestReader(HttpRequest(uri = uri, method = HttpMethods.POST, entity = e))
        .flatMap(response =>
          HttpResponseHandler.handlerResponse[String, String](s"${config.serviceName}:$uri", response))
    }
  }

  def createSegment(segment: Segment): Future[ApiResponse[String, String]] = {
    segment match {
      case p: ProductSegment =>
        Marshal(Conversions.productSegmentToRequest(p)).to[RequestEntity].flatMap { e =>
          val uri = s"${config.host}/api/segments/product"
          singleRequestReader(HttpRequest(uri = uri, method = HttpMethods.POST, entity = e))
            .flatMap(response =>
              HttpResponseHandler.handlerResponse[String, String](s"${config.serviceName}:$uri", response))
        }
    }
  }

  def getSegment(id: String): Future[ApiResponse[ProductSegment, String]] = {
    val uri = s"${config.host}/api/segments/product/$id"
    singleRequestReader(HttpRequest(uri = uri, method = HttpMethods.GET))
      .flatMap(response =>
        HttpResponseHandler.handlerResponse[ProductSegmentDto, String](s"${config.serviceName}:$uri", response))
      .map(_.map(Conversions.toProductSegment))
  }

  def updateSegment(uriPath: String,
                    segment: Segment,
                    items: Set[SkuItem]): Future[ApiResponse[ProductSegment, String]] = {
    def patchRequest(uri: String,
                    items: Set[SkuItem]): Future[ApiResponse[ProductSegment, String]] = {
      Marshal(Conversions.skuItemsToRequest(items)).to[RequestEntity].flatMap { e =>
        singleRequestReader(HttpRequest(uri = uri, method = HttpMethods.PATCH, entity = e))
          .flatMap(response =>
            HttpResponseHandler.handlerResponse[ProductSegmentDto, String](s"${config.serviceName}:$uri", response))
          .map(_.map(Conversions.toProductSegment))
      }
    }

    Identification.namespaceForSegment(segment.descriptor)
      .map(uuid => patchRequest(s"${config.host}/api/segments/product/${uuid.toString}/$uriPath", items))
      .getOrElse(Future.failed(new Exception(s"Identification.namespaceForSegment failed for $segment")))
  }

  def addSegmentItems(segment: Segment,
                      items: Set[SkuItem]): Future[ApiResponse[ProductSegment, String]] =
    updateSegment("add/items", segment, items)

  def removeSegmentItems(segment: Segment,
                         items: Set[SkuItem]): Future[ApiResponse[ProductSegment, String]] =
    updateSegment("remove/items", segment, items)
}

