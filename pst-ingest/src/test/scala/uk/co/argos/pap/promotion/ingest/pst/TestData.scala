package uk.co.argos.pap.promotion.ingest.pst

object TestData {

  val  promotions =
    """
      |[
      |    {
      |        "lastUpdated": 1575816582283,
      |        "stackingRules": {
      |            "stackingRule": {
      |                "type": "Combines"
      |            },
      |            "promotionPriority": 0
      |        },
      |        "descriptor": {
      |            "id": "6b31542d-fa7b-4820-8e13-51d5defa1c16",
      |            "name": "1000NectarPointsIf50",
      |            "description": "testDescription",
      |            "expression": "NECTAR 1000 IN Basket IF > £50 IN Basket"
      |        },
      |        "dateRange": {
      |            "from": "2018-12-01T13:12:28.003Z",
      |            "to": "2019-12-03T13:12:28.002Z"
      |        },
      |        "promotionExpression": {
      |            "type": "OrderLevelPromotion",
      |            "promotionType": {
      |                "type": "Nectar",
      |                "nectarReward": {
      |                    "type": "NectarPoints",
      |                    "value": 1000
      |                }
      |            },
      |            "segmentOperator": {
      |                "type": "AND",
      |                "left": {
      |                    "type": "IN",
      |                    "descriptor": {
      |                        "type": "BasketSegmentDescriptor",
      |                        "subject": []
      |                    }
      |                },
      |                "right": {
      |                    "type": "Qualification",
      |                    "segmentOperator": {
      |                        "type": "MatchOperator",
      |                        "matcher": {
      |                            "type": "GT"
      |                        },
      |                        "value": {
      |                            "type": "Value",
      |                            "value": 50
      |                        },
      |                        "operator": {
      |                            "type": "IN",
      |                            "descriptor": {
      |                                "type": "BasketSegmentDescriptor",
      |                                "subject": []
      |                            }
      |                        }
      |                    }
      |                }
      |            }
      |        },
      |        "created": 1575816582283
      |    }
      |]
      |""".stripMargin

}
