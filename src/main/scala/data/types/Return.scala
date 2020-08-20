package data.types

/**
  * Return Entity
  *
  * @since 3.0
  *
  */
case class Return(customerId: Int,
                  gender: Option[String],
                  returnId: Int,
                  returnItemId: Option[String],
                  returnsDateKey: Long,
                  receiptId: Int,
                  receiptItemId: Option[String],
                  receiptDateKey: Long,
                  optionCode: Option[String],
                  parentSkuCode: Option[String],
                  childSkuCode: Option[String],
                  statusId: Option[String],
                  divisionId: Option[Int],
                  brandDesc: Option[String],
                  sizeId: Option[String],
                  sizeDesc: Option[String],
                  colourId: Option[String],
                  colourDesc: Option[String],
                  actualColourDesc: Option[String],
                  itemQty: Option[String],
                  voidAction: Option[String],
                  voidReason: Option[String],
                  replacementReceiptId: Option[String],
                  discountPercentage: Option[String],
                  priceAtPurchase: Option[String],
                  dateEntered: Option[Long],
                  dateModified: Option[Long])
