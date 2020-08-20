package data.types

/**
  * GenericSignal entity
  *
  * @since 4.0.2
  */
case class GenericSignal(customerId: Int,
                         productId: Int,
                         variantId: Int,
                         divisionId: Int,
                         sourceId: Int,
                         itemQty: Option[Int],
                         signalDate: Long,
                         origin: String,
                         price: Option[Double],
                         discountType: String,
                         useForRecs: Int,
                         dateModified: Long)
``