package uk.co.argos.pap.promotion.ingest.util

import kamon.Kamon
import kamon.metric.{Counter, Metric}

// This is a fix, Kamon needs to be init before everything else - https://github.com/kamon-io/Kamon/issues/601
trait KamonMetrics {
  Kamon.init()
}

object Metrics {
  def incrementCounterMetric(name: String,
                             times: Long = 1): Counter = {
    val counter: Metric.Counter = Kamon.counter(name)
    counter.withoutTags()
      .increment(times)
  }
}