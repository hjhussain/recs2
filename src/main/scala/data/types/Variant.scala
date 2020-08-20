package data.types

/**
  * Variant entity
  *
  * @since 3.0
  */
case class Variant(productId: Int,
                   variantId: Int,
                   statusId: Int,
                   sizeId: Int,
                   sizeName: String,
                   colourCodeId: Option[String],
                   colourName: String, supplierColourName: String,
                   currentPrice: Option[Double],
                   rrp: Option[Double],
                   sku: String,
                   dateModified: Long,
                   stockLevel: Option[Int])
