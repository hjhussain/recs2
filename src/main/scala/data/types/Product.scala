package data.types

/**
  * Product entity
  *
  * @since 3.0
  *
  */
case class Product(productId: Int,
                   name: String,
                   brandName: String,
                   brandId: Int,
                   category: String,
                   departmentId: Int,
                   department: String,
                   divisionId: Int,
                   division: String,
                   season: Option[String],
                   dateOnSite: Option[String],
                   statusId: Int,
                   currentPrice: Double,
                   dateModified: String,
                   stockLevel: Option[BigInt],
                   productType: Option[String],
                   productTypeId: Option[Int],
                   highLevelProductType: String,
                   highLevelProductTypeId: Int,
                   brandFamily: String,
                   isAsos: Int,
                   isMaternity: Int,
                   isPolarizing: String,
                   isExcluded: Int,
                   isChristmas: Int)
