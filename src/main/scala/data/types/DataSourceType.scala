package data.types

import com.de.recs.common.FieldNames._
import org.apache.spark.sql.types._

/**
  * Base class for defining Data Set types
  *
  * @since 2.0
  */
trait DataSourceType {

  /**
    * override this method to return schema of DataSourceType
    *
    * @return StructType - Schema of Data Set
    */
  def schema(): StructType
}

/**
  * Customer Data Type. Schema is shown below.<br/><br/>
  * <code>
  *
  * root<br/>
  * |-- customerId: integer (nullable = true)<br/>
  * |-- guid: string (nullable = true)<br/>
  * |-- gender: string (nullable = true)<br/>
  * |-- shippingCountry: string (nullable = true)<br/>
  * |-- dateEntered: long (nullable = true)<br/>
  * |-- dateModified: long (nullable = true)<br/>
  * |-- yearOfBirth: integer (nullable = true)<br/>
  * |-- premier: integer (nullable = true)<br/>
  * <br/>
  * </code>
  *
  * @since 2.0
  */
object CustomerType extends DataSourceType {

  /**
    * Schema of Customer
    *
    * @return StructType - Schema of Customer
    */
  override def schema(): StructType = {
    StructType(Array(
      StructField(FLD_CUSTOMER_ID, IntegerType, false),
      StructField(FLD_GUID, StringType, true),
      StructField(FLD_GENDER, StringType, true),
      StructField(FLD_SHIPPING_COUNTRY, StringType, true),
      StructField(FLD_DATE_ENTERED, StringType, true),
      StructField(FLD_DATE_MODIFIED, StringType, true),
      StructField(FLD_YEAR_OF_BIRTH, IntegerType, true),
      StructField(FLD_PREMIER, IntegerType, true)
    ))
  }
}

/**
  *
  * Product Data Type. Schema is shown below.<br/><br/>
  * <code>
  *
  * root<br/>
  * |-- productId: integer (nullable = true)<br/>
  * |-- name: string (nullable = true)<br/>
  * |-- brandName: string (nullable = true)<br/>
  * |-- brandId: integer (nullable = true)<br/>
  * |-- category: string (nullable = true)<br/>
  * |-- departmentId: integer (nullable = true)<br/>
  * |-- department: string (nullable = true)<br/>
  * |-- divisionId: integer (nullable = true)<br/>
  * |-- division: string (nullable = true)<br/>
  * |-- season: string (nullable = true)<br/>
  * |-- dateOnSite: string (nullable = true)<br/>
  * |-- statusId: integer (nullable = true)<br/>
  * |-- currentPrice: float (nullable = true)<br/>
  * |-- dateModified: string (nullable = true)<br/>
  * |-- stockLevel: integer (nullable = true)<br/>
  * |-- productType: string (nullable = true)<br/>
  * |-- productTypeId: integer (nullable = true)<br/>
  * |-- highLevelProductType: string (nullable = true)<br/>
  * |-- highLevelProductTypeId: integer (nullable = true)<br/>
  * |-- brandFamily: string (nullable = true)<br/>
  * |-- isAsos: integer (nullable = true)<br/>
  * |-- isMaternity: integer (nullable = true)<br/>
  * |-- isPolarizing: string (nullable = true)<br/>
  * |-- isExcluded: integer (nullable = true)<br/>
  * |-- isChristmas: integer (nullable = true)<br/>
  * <br/>
  * </code>
  *
  * @since 2.0
  */
object ProductType extends DataSourceType {

  /**
    * Schema of Product
    *
    * @return StructType - Schema of Product
    */
  override def schema(): StructType = {
    StructType(Array(
      StructField(FLD_PRODUCT_ID, IntegerType, false),
      StructField(FLD_NAME, StringType, true),
      StructField(FLD_BRAND_NAME, StringType, true),
      StructField(FLD_BRAND_ID, IntegerType, false),
      StructField(FLD_CATEGORY, StringType, true),
      StructField(FLD_DEPARTMENT_ID, IntegerType, false),
      StructField(FLD_DEPARTMENT, StringType, true),
      StructField(FLD_DIVISION_ID, IntegerType, false),
      StructField(FLD_DIVISION, StringType, true),
      StructField(FLD_SEASON, StringType, true),
      StructField(FLD_DATE_ON_SITE, StringType, true),
      StructField(FLD_STATUS_ID, IntegerType, false),
      StructField(FLD_CURRENT_PRICE, DoubleType, false),
      StructField(FLD_DATE_MODIFIED, StringType, true),
      StructField(FLD_STOCK_LEVEL, IntegerType, true),
      StructField(FLD_PRODUCT_TYPE, StringType, true),
      StructField(FLD_PRODUCT_TYPE_ID, IntegerType, true),
      StructField(FLD_HIGH_LEVEL_PRODUCT_TYPE, StringType, true),
      StructField(FLD_HIGH_LEVEL_PRODUCT_TYPE_ID, IntegerType, false),
      StructField(FLD_BRAND_FAMILY, StringType, true),
      StructField(FLD_IS_ASOS, IntegerType, false),
      StructField(FLD_IS_MATERNITY, IntegerType, false),
      StructField(FLD_IS_POLARIZING, StringType, true),
      StructField(FLD_IS_EXCLUDED, IntegerType, false),
      StructField(FLD_IS_CHRISTMAS, IntegerType, false)
    ))
  }
}

/**
  * Product Variants Data Type. Schema is shown below.<br/><br/>
  * <code>
  * root<br/>
  * |-- productId: integer (nullable = true)<br/>
  * |-- variantId: integer (nullable = true)<br/>
  * |-- statusId: integer (nullable = true)<br/>
  * |-- sizeId: integer (nullable = true)<br/>
  * |-- sizeName: string (nullable = true)<br/>
  * |-- colourCodeId: string (nullable = true)<br/>
  * |-- colourName: string (nullable = true)<br/>
  * |-- supplierColourName: string (nullable = true)<br/>
  * |-- currentPrice: float (nullable = true)<br/>
  * |-- rrp: float (nullable = true)<br/>
  * |-- sku: string (nullable = true)<br/>
  * |-- dateModified: string (nullable = true)<br/>
  * |-- stockLevel: integer (nullable = true)<br/>
  * <br/>
  * </code>
  *
  * @since 2.0
  */
object VariantType extends DataSourceType {

  /**
    * Schema of Variants
    *
    * @return StructType - Schema of Variants
    */
  override def schema(): StructType = {
    StructType(Array(
      StructField(FLD_PRODUCT_ID, IntegerType, false),
      StructField(FLD_VARIANT_ID, IntegerType, false),
      StructField(FLD_STATUS_ID, IntegerType, false),
      StructField(FLD_SIZE_ID, IntegerType, false),
      StructField(FLD_SIZE_NAME, StringType, false),
      StructField(FLD_COLOR_CODE_ID, StringType, true),
      StructField(FLD_COLOR_NAME, StringType, true),
      StructField(FLD_SUPPLIER_COLOUR_NAME, StringType, true),
      StructField(FLD_CURRENT_PRICE, DoubleType, true),
      StructField(FLD_RRP, DoubleType, true),
      StructField(FLD_SKU, StringType, false),
      StructField(FLD_DATE_MODIFIED, StringType, false),
      StructField(FLD_STOCK_LEVEL, IntegerType, true)
    ))
  }
}

/**
  * Receipts Data Type. Schema is shown below.<br/><br/>
  * <code>
  * root<br/>
  * |-- customerId: integer (nullable = true)<br/>
  * |-- gender: string (nullable = true)<br/>
  * |-- productId: integer (nullable = true)<br/>
  * |-- optionCode: string (nullable = true)<br/>
  * |-- parentSkuCode: string (nullable = true)<br/>
  * |-- childSkuCode: string (nullable = true)<br/>
  * |-- receiptId: integer (nullable = true)<br/>
  * |-- statusId: integer (nullable = true)<br/>
  * |-- divisionId: integer (nullable = true)<br/>
  * |-- signalDate: integer (nullable = true)<br/>
  * |-- receiptSeasonDesc: string (nullable = true)<br/>
  * |-- seasonStatusDesc: string (nullable = true)<br/>
  * |-- brandDesc: string (nullable = true)<br/>
  * |-- sizeId: integer (nullable = true)<br/>
  * |-- sizeDesc: string (nullable = true)<br/>
  * |-- colourId: integer (nullable = true)<br/>
  * |-- colourDesc: string (nullable = true)<br/>
  * |-- actualColourDesc: string (nullable = true)<br/>
  * |-- itemQty: integer (nullable = true)<br/>
  * |-- shippingCountryName: string (nullable = true)<br/>
  * |-- shippingCountryCode: string (nullable = true)<br/>
  * |-- discountSubGroup: string (nullable = true)<br/>
  * |-- receiptStatusId: integer (nullable = true)<br/>
  * |-- discountPercentage: string (nullable = true)<br/>
  * |-- priceAtPurchase: double (nullable = true)<br/>
  * |-- paymentMethod: string (nullable = true)<br/>
  * |-- shippingMethod: string (nullable = true)<br/>
  * |-- shippingAmount: string (nullable = true)<br/>
  * |-- isPremier: boolean (nullable = true)<br/>
  * |-- dateEntered: string (nullable = true)<br/>
  * |-- dateModified: string (nullable = true)<br/>
  *
  * </code>
  *
  * @since 2.0
  */
object ReceiptType extends DataSourceType {
  /**
    * Schema of Variants
    *
    * @return StructType - Schema of Variants
    */
  override def schema(): StructType = {
    StructType(Array(
      StructField(FLD_CUSTOMER_ID, IntegerType, false),
      StructField(FLD_GENDER, StringType, false),
      StructField(FLD_PRODUCT_ID, IntegerType, false),
      StructField(FLD_OPTION_CODE, StringType, false),
      StructField(FLD_PARENT_SKU_CODE, StringType, false),
      StructField(FLD_CHILD_SKU_CODE, StringType, false),
      StructField(FLD_RECEIPT_ID, IntegerType, false),
      StructField(FLD_STATUS_ID, IntegerType, true),
      StructField(FLD_DIVISION_ID, IntegerType, true),
      StructField(FLD_SIGNAL_DATE, IntegerType, true),
      StructField(FLD_RECEIPT_SEASON_DESC, StringType, false),
      StructField(FLD_RECEIPT_STATUS_DESC, StringType, false),
      StructField(FLD_BRAND_DESC, StringType, false),
      StructField(FLD_SIZE_ID, IntegerType, true),
      StructField(FLD_SIZE_DESC, StringType, true),
      StructField(FLD_COLOUR_ID, IntegerType, true),
      StructField(FLD_COLOUR_DESC, StringType, true),
      StructField(FLD_ACTUAL_COLOUR_DESC, StringType, true),
      StructField(FLD_ITEM_QTY, IntegerType, true),
      StructField(FLD_SHIPPING_COUNTRY_NAME, StringType, false),
      StructField(FLD_SHIPPING_COUNTRY_CODE, StringType, false),
      StructField(FLD_DISCOUNT_SUB_GROUP, StringType, true),
      StructField(FLD_RECEIPT_STATUS_ID, IntegerType, true),
      StructField(FLD_DISCOUNT_PERCENTAGE, StringType, true),
      StructField(FLD_PRICE_AT_PURCHASE, DoubleType, true),
      StructField(FLD_PAYMENT_METHOD, StringType, false),
      StructField(FLD_SHIPPING_METHOD, StringType, false),
      StructField(FLD_SHIPPING_AMOUNT, StringType, true),
      StructField(FLD_IS_PREMIER, BooleanType, false),
      StructField(FLD_DATE_ENTERED, StringType, false),
      StructField(FLD_DATE_MODIFIED, StringType, false)
    ))
  }
}

/**
  * Returns Data Type. Schema is shown below.<br/><br/>
  * <code>
  * root<br/>
  * |-- customerId: integer (nullable = true)<br/>
  * |-- gender: string (nullable = true)<br/>
  * |-- returnId: integer (nullable = true)<br/>
  * |-- returnItemId: string (nullable = true)<br/>
  * |-- returnsDateKey: string (nullable = true)<br/>
  * |-- receiptId: integer (nullable = true)<br/>
  * |-- receiptItemId: string (nullable = true)<br/>
  * |-- receiptDateKey: string (nullable = true)<br/>
  * |-- optionCode: string (nullable = true)<br/>
  * |-- parentSkuCode: string (nullable = true)<br/>
  * |-- childSkuCode: string (nullable = true)<br/>
  * |-- statusId: string (nullable = true)<br/>
  * |-- divisionId: integer (nullable = true)<br/>
  * |-- brandDesc: string (nullable = true)<br/>
  * |-- sizeId: string (nullable = true)<br/>
  * |-- sizeDesc: string (nullable = true)<br/>
  * |-- colourId: string (nullable = true)<br/>
  * |-- colourDesc: string (nullable = true)<br/>
  * |-- actualColourDesc: string (nullable = true)<br/>
  * |-- itemQty: string (nullable = true)<br/>
  * |-- voidAction: string (nullable = true)<br/>
  * |-- voidReason: string (nullable = true)<br/>
  * |-- replacementReceiptId: string (nullable = true)<br/>
  * |-- discountPercentage: string (nullable = true)<br/>
  * |-- priceAtPurchase: string (nullable = true)<br/>
  * |-- dateEntered: string (nullable = true)<br/>
  * |-- dateModified: string (nullable = true)<br/>
  * <br/>
  * </code>
  *
  * @since 2.0
  */
object ReturnType extends DataSourceType {
  /**
    * Schema of Returns
    *
    * @return StructType - Schema of Returns
    */
  override def schema(): StructType = {
    StructType(Array(
      StructField(FLD_CUSTOMER_ID, IntegerType, false),
      StructField(FLD_GENDER, StringType, true),
      StructField(FLD_RETURN_ID, IntegerType, false),
      StructField(FLD_RETURN_ITEM_ID, StringType, true),
      StructField(FLD_RETURNS_DATE_KEY, StringType, false),
      StructField(FLD_RECEIPT_ID, IntegerType, false),
      StructField(FLD_RECEIPT_ITEM_ID, StringType, true),
      StructField(FLD_RECEIPT_DATE_KEY, StringType, false),
      StructField(FLD_OPTION_CODE, StringType, true),
      StructField(FLD_PARENT_SKU_CODE, StringType, true),
      StructField(FLD_CHILD_SKU_CODE, StringType, true),
      StructField(FLD_STATUS_ID, StringType, true),
      StructField(FLD_DIVISION_ID, IntegerType, true),
      StructField(FLD_BRAND_DESC, StringType, true),
      StructField(FLD_SIZE_ID, StringType, true),
      StructField(FLD_SIZE_DESC, StringType, true),
      StructField(FLD_COLOUR_ID, StringType, true),
      StructField(FLD_COLOUR_DESC, StringType, true),
      StructField(FLD_ACTUAL_COLOUR_DESC, StringType, true),
      StructField(FLD_ITEM_QTY, StringType, true),
      StructField(FLD_VOID_ACTION, StringType, true),
      StructField(FLD_VOID_REASON, StringType, true),
      StructField(FLD_REPLACEMENT_RECEIPT_ID, StringType, true),
      StructField(FLD_DISCOUNT_PERCENTAGE, StringType, true),
      StructField(FLD_PRICE_AT_PURCHASE, StringType, true),
      StructField(FLD_DATE_ENTERED, StringType, true),
      StructField(FLD_DATE_MODIFIED, StringType, true)))
  }
}
