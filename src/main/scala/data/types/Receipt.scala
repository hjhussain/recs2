package data.types

/**
  * Receipt entity
  *
  * @since 3.0
  */
case class Receipt(customerId: Option[Int],
                   gender: String,
                   productId: Int,
                   optionCode: String,
                   parentSkuCode: String,
                   childSkuCode: String,
                   receiptId: Int,
                   statusId: Option[Int],
                   divisionId: Option[Int],
                   signalDate: Option[Int],
                   receiptSeasonDesc: String,
                   seasonStatusDesc: String,
                   brandDesc: String,
                   sizeId: Option[Int],
                   sizeDesc: Option[String],
                   colourId: Option[Int],
                   colourDesc: Option[String],
                   actualColourDesc: Option[String],
                   itemQty: Option[Int],
                   shippingCountryName: String,
                   shippingCountryCode: String,
                   discountSubGroup: Option[String],
                   receiptStatusId: Option[Int],
                   discountPercentage: Option[String],
                   priceAtPurchase: Option[Double],
                   paymentMethod: String,
                   shippingMethod: String,
                   shippingAmount: Option[String],
                   isPremier: Boolean,
                   dateEntered: Long,
                   dateModified: Long)
