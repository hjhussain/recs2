package data

import data.customer.CustomerLoader
import data.product.ProductLoader
import data.receipt.ReceiptLoader
import data.returns.ReturnLoader
import data.signal.GenericSignalLoader
import data.variant.VariantLoader


/**
  * This is the entry point to get access to various Data Loaders.<br/> It is a factory for Data Loaders.<br/><br/>
  * How to Use <br/>
  * <code>
  * val customerLoader = DataLoader.customerLoader <br/>
  * val customerDF = customerLoader.loadParquet(sqlContext, customersParquetPath) <br/>
  * </code>
  *
  * @since 2.0
  */
object DataLoader {

  /**
    * Returns a Customer Data Loader
    *
    * @since 2.0
    */
  def customerLoader: CustomerLoader.type = CustomerLoader

  /**
    *
    * Returns a Product Data Loader
    *
    * @since 2.0
    */
  def productLoader: ProductLoader.type = ProductLoader

  /**
    * Returns a Receipts Data Loader
    *
    * @since 2.0
    */
  def receiptLoader: ReceiptLoader.type = ReceiptLoader

  /**
    * Returns a Return Data Loader
    *
    * @since 2.0
    */
  def returnLoader: ReturnLoader.type = ReturnLoader

  /**
    * Returns Variant Data Loader
    *
    * @since 2.0
    */
  def variantLoader: VariantLoader.type = VariantLoader

  /**
    * Returns Signal Data Loader
    *
    * @since 4.0.2
    */
  def signalsLoader: GenericSignalLoader.type = GenericSignalLoader
}
