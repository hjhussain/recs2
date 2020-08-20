package uk.co.argos.pap.promotion.ingest.util

import java.util.concurrent.TimeUnit

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


object RunUtil {


  def boundedExecutionContext(capacity: Int = 200, parallelism: Int  = 10): ExecutionContextExecutor = {

    import java.util.concurrent.{LinkedBlockingDeque, ThreadPoolExecutor}
    val linkedBlockingDeque = new LinkedBlockingDeque[Runnable](capacity)
    val executorService = new ThreadPoolExecutor(1, parallelism, 50, TimeUnit.SECONDS, linkedBlockingDeque,
      new ThreadPoolExecutor.CallerRunsPolicy)

    ExecutionContext.fromExecutor(executorService)
  }


}
