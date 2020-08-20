package data.types

/**
  * Customer entity
  *
  * @since 3.0
  */
case class Customer(customerId: Int,
                    guid: String,
                    gender: String,
                    shippingCountry: String,
                    dateEntered: Option[Long],
                    dateModified: Option[Long],
                    yearOfBirth: Int,
                    premier: Int)
