package com.de.recs.common

case class ItemFactors(id: Int, factors: Array[Double])

case class SimilarityScore(id: Int, similarId: Int, score: Double)
