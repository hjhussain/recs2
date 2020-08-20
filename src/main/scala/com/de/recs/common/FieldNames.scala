package com.de.recs.common

/**
  * Using hard coded string literals for field names can lead to lot of errors because of typos.<br/>
  * This provides a solution for that by providing constants for commonly used field names.
  *
  * @since 1.0
  */
object FieldNames {

  // field names for receipts
  /**
    * Value customerId
    */
  val FLD_CUSTOMER_ID = "customerId"
  /**
    * Value gender
    */
  val FLD_GENDER = "gender"
  /**
    * Value sourceId
    */
  val FLD_SOURCE_ID = "sourceId"
  /**
    * Value signalDate
    */
  val FLD_SIGNAL_DATE = "signalDate"
  /**
    * Value dateEntered
    */
  val FLD_DATE_ENTERED = "dateEntered"
  /**
    * Value discountType
    */
  val FLD_DISCOUNT_TYPE = "discountType"
  /**
    * Value useForRecs
    */
  val FLD_USE_FOR_RECS = "useForRecs"
  /**
    * Value itemQty
    */
  val FLD_ITEM_QUANTITY = "itemQty"
  /**
    * Value origin
    */
  val FLD_ORIGIN = "origin"
  /**
    * Value discountSubGroup
    */
  val FLD_DISCOUNT_SUB_GROUP = "discountSubGroup"
  /**
    * Value receiptId
    */
  val FLD_RECEIPT_ID = "receiptId"
  /**
    * Value optionCode
    */
  val FLD_OPTION_CODE = "optionCode"
  /**
    * Value parentSkuCode
    */
  val FLD_PARENT_SKU_CODE = "parentSkuCode"
  /**
    * Value receiptSeasonDesc
    */
  val FLD_RECEIPT_SEASON_DESC = "receiptSeasonDesc"
  /**
    * Value seasonStatusDesc
    */
  val FLD_RECEIPT_STATUS_DESC = "seasonStatusDesc"
  /**
    * Value shippingCountryName
    */
  val FLD_SHIPPING_COUNTRY_NAME = "shippingCountryName"
  /**
    * Value shippingCountryCode
    */
  val FLD_SHIPPING_COUNTRY_CODE = "shippingCountryCode"
  /**
    * Value receiptStatusId
    */
    val FLD_RECEIPT_STATUS_ID = "receiptStatusId"
  /**
    * Value paymentMethod
    */
  val FLD_PAYMENT_METHOD = "paymentMethod"
  /**
    * Value shippingMethod
    */
  val FLD_SHIPPING_METHOD = "shippingMethod"
  /**
    * Value shippingAmount
    */
  val FLD_SHIPPING_AMOUNT = "shippingAmount"
  /**
    * Value isPremier
    */
  val FLD_IS_PREMIER = "isPremier"

  // FIELD NAMES FOR RETURNS
  /**
    * Value returnId
    */
  val FLD_RETURN_ID = "returnId"
  /**
    * Value returnItemId
    */
  val FLD_RETURN_ITEM_ID = "returnItemId"
  /**
    * Value returnsDateKey
    */
  val FLD_RETURNS_DATE_KEY = "returnsDateKey"
  /**
    * Value receiptItemId
    */
  val FLD_RECEIPT_ITEM_ID = "receiptItemId"
  /**
    * Value receiptDateKey
    */
  val FLD_RECEIPT_DATE_KEY = "receiptDateKey"
  /**
    * Value brandDesc
    */
  val FLD_BRAND_DESC = "brandDesc"
  /**
    * Value sizeDesc
    */
  val FLD_SIZE_DESC = "sizeDesc"
  /**
    * Value colourId
    */
  val FLD_COLOUR_ID = "colourId"
  /**
    * Value colourDesc
    */
  val FLD_COLOUR_DESC = "colourDesc"
  /**
    * Value actualColourDesc
    */
  val FLD_ACTUAL_COLOUR_DESC = "actualColourDesc"
  /**
    * Value itemQty
    */
  val FLD_ITEM_QTY = "itemQty"
  /**
    * Value voidAction
    */
  val FLD_VOID_ACTION = "voidAction"
  /**
    * Value voidReason
    */
  val FLD_VOID_REASON = "voidReason"
  /**
    * Value replacementReceiptId
    */
  val FLD_REPLACEMENT_RECEIPT_ID = "replacementReceiptId"
  /**
    * Value priceAtPurchase
    */
  val FLD_PRICE_AT_PURCHASE = "priceAtPurchase"
  /**
    * Value discountPercentage
    */
  val FLD_DISCOUNT_PERCENTAGE = "discountPercentage"

  // Field names for product
  /**
    * Value brandName
    */
  val FLD_BRAND_NAME = "brandName"
  /**
    * Value brandId
    */
  val FLD_BRAND_ID = "brandId"
  /**
    * Value category
    */
  val FLD_CATEGORY = "category"
  /**
    * Value departmentId
    */
  val FLD_DEPARTMENT_ID = "departmentId"
  /**
    * Value department
    */
  val FLD_DEPARTMENT = "department"
  /**
    * Value divisionId
    */
  val FLD_DIVISION_ID = "divisionId"
  /**
    * Value division
    */
  val FLD_DIVISION = "division"
  /**
    * Value dateOnSite
    */
  val FLD_DATE_ON_SITE = "dateOnSite"
  /**
    * Value statusId
    */
  val FLD_STATUS_ID = "statusId"
  /**
    * Value sku
    */
  val FLD_SKU = "sku"
  /**
    * Value childSkuCode
    */
  val FLD_CHILD_SKU_CODE = "childSkuCode"
  /**
    * Value rrp
    */
  val FLD_RRP = "rrp"
  /**
    * Value currentPrice
    */
  val FLD_CURRENT_PRICE = "currentPrice"
  /**
    * Value dateModified
    */
  val FLD_DATE_MODIFIED = "dateModified"
  /**
    * Value productId
    */
  val FLD_PRODUCT_ID = "productId"
  /**
    * Value season
    */
  val FLD_SEASON = "season"
  /**
    * Value price
    */
  val FLD_PRICE = "price"
  /**
    * Value productType
    */
  val FLD_PRODUCT_TYPE = "productType"
  /**
    * Value productTypeId
    */
  val FLD_PRODUCT_TYPE_ID = "productTypeId"
  /**
    * Value brandFamily
    */
  val FLD_BRAND_FAMILY = "brandFamily"
  /**
    * Value brandFamilyCode
    */
  val FLD_BRAND_FAMILY_CODE = "brandFamilyCode"
  /**
    * Value isAsos
    */
  val FLD_IS_ASOS = "isAsos"
  /**
    * Value discounted
    */
  val FLD_DISCOUNTED = "discounted"
  /**
    * Value isRecommended
    */
  val FLD_IS_RECOMMENDED = "isRecommended"
  /**
    * Value attributes
    */
  val FLD_ATTRIBUTES = "attributes"
  /**
    * Value name
    */
  val FLD_NAME = "name"
  /**
    * Value nameId
    */
  val FLD_NAME_ID = "nameId"
  /**
    * Value stockLevel
    */
  val FLD_STOCK_LEVEL = "stockLevel"
  /**
    * Value webCat
    */
  val FLD_WEB_CAT = "webCat"
  /**
    * Value webCatId
    */
  val FLD_WEB_CAT_ID = "webCatId"
  /**
    * Value highLevelProductType
    */
  val FLD_HIGH_LEVEL_PRODUCT_TYPE = "highLevelProductType"
  /**
    * Value highLevelProductTypeId
    */

  val FLD_HIGH_LEVEL_PRODUCT_TYPE_ID = "highLevelProductTypeId"
  /**
    * Value highLevelProductTypeCode
    */
  val FLD_HIGH_LEVEL_PRODUCT_TYPE_CODE = "highLevelProductTypeCode"
  /**
    * Value isMaternity
    */
  val FLD_IS_MATERNITY = "isMaternity"
  /**
    * Value isChristmas
    */
  val FLD_IS_CHRISTMAS = "isChristmas"
  /**
    * Value isPolarizing
    */
  val FLD_IS_POLARIZING = "isPolarizing"
  /**
    * Value isExcluded
    */
  val FLD_IS_EXCLUDED = "isExcluded"
  /**
    * Value variantId
    */
  val FLD_VARIANT_ID = "variantId"
  /**
    * Value polarizingValues
    */
  val FLD_POLARIZING_VALUES = "polarizingValues"

  // Field names for Customer Combined
  /**
    * Value shippingCountry
    */
  val FLD_SHIPPING_COUNTRY = "shippingCountry"
  /**
    * Value premier
    */
  val FLD_PREMIER = "premier"
  /**
    * Value guid
    */
  val FLD_GUID = "guid"
  /**
    * Value yearOfBirth
    */
  val FLD_YEAR_OF_BIRTH = "yearOfBirth"

  // Field names for Variants
  /**
    * Value colourCodeId
    */
  val FLD_COLOR_CODE_ID = "colourCodeId"
  /**
    * Value colourName
    */
  val FLD_COLOR_NAME = "colourName"
  /**
    * Value supplierColourName
    */
  val FLD_SUPPLIER_COLOUR_NAME = "supplierColourName"
  /**
    * Value sizeId
    */
  val FLD_SIZE_ID = "sizeId"
  /**
    * Value sizeName
    */
  val FLD_SIZE_NAME = "sizeName"
  /**
    * Value variants
    */
  val FLD_VARIANTS = "variants"

  /**
    * Value analyticalDatasetId
    */
  val FLD_ANALYTICAL_DATASET_ID = "analyticalDatasetId"
  /**
    * Value itemId
    */
  val FLD_ITEM_ID = "itemId"
  /**
    * Value itemId1
    */
  val FLD_ITEM_ID_1 = "itemId1"
  /**
    * Value itemId2
    */
  val FLD_ITEM_ID_2 = "itemId2"
  /**
    * Value value
    */
  val FLD_VALUE = "value"
  /**
    * Value classId
    */
  val FLD_CLASS_ID = "classId"
  /**
    * Value effectiveFrom
    */
  val FLD_EFFECTIVE_FROM = "effectiveFrom"
  /**
    * Value effectiveTo
    */
  val FLD_EFFECTIVE_TO = "effectiveTo"
  /**
    * Value generatedOn
    */
  val FLD_GENERATED_ON = "generatedOn"


}
