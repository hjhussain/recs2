package data.types

import com.de.recs.common.FieldNames._
import org.apache.spark.sql.types._

/**
  * GenericSignalType Data Type. Schema is shown below.<br/><br/>
  * <code>
  * root<br/>
  * |-- customerId: integer (nullable = false)<br/>
  * |-- productId: integer (nullable = false)<br/>
  * |-- variantId: integer (nullable = false)<br/>
  * |-- divisionId: integer (nullable = false)<br/>
  * |-- sourceId: integer (nullable = false)<br/>
  * |-- itemQty: integer (nullable = true)<br/>
  * |-- signalDate: string (nullable = true)<br/>
  * |-- origin: string (nullable = true)<br/>
  * |-- price: double (nullable = false)<br/>
  * |-- discountType: string (nullable = true)<br/>
  * |-- useForRecs: integer (nullable = false)<br/>
  * |-- dateModified: string (nullable = true)<br/>
  * <br/>
  * </code>
  *
  * @since 4.0.2
  */

object GenericSignalType extends DataSourceType {

  override def schema(): StructType = {
    StructType(Array(
      StructField(FLD_CUSTOMER_ID, IntegerType, false),
      StructField(FLD_PRODUCT_ID, IntegerType, false),
      StructField(FLD_VARIANT_ID, IntegerType, false),
      StructField(FLD_DIVISION_ID, IntegerType, false),
      StructField(FLD_SOURCE_ID, IntegerType, false),
      StructField(FLD_ITEM_QTY, IntegerType, true),
      StructField(FLD_SIGNAL_DATE, StringType, false),
      StructField(FLD_ORIGIN, StringType, true),
      StructField(FLD_PRICE, DoubleType, false),
      StructField(FLD_DISCOUNT_TYPE, StringType, true),
      StructField(FLD_USE_FOR_RECS, IntegerType, false),
      StructField(FLD_DATE_MODIFIED, StringType, true)
    ))
  }
}
