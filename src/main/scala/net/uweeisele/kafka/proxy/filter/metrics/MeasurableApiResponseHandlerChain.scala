package net.uweeisele.kafka.proxy.filter.metrics

import io.micrometer.core.instrument.{MeterRegistry, Timer}
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.response.ApiResponseHandler

import java.io.Closeable
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES}
import scala.jdk.DurationConverters.ScalaDurationOps

object MeasurableApiResponseHandlerChain {
  def apply(handlerChain: Seq[ApiResponseHandler],
            prefix: String,
            ttl: FiniteDuration = Duration(5, MINUTES))
           (implicit meterRegistry: MeterRegistry): MeasurableApiResponseHandlerChain =
    new MeasurableApiResponseHandlerChain(handlerChain, meterRegistry, prefix, ttl)
}

class MeasurableApiResponseHandlerChain(handlerChain: Seq[ApiResponseHandler],
                                        meterRegistry: MeterRegistry,
                                        prefix: String,
                                        ttl: FiniteDuration = Duration(5, MINUTES))
  extends ApiResponseHandler with Closeable {

  private val timer: Timer = Timer.builder(s"$prefix.responsehandler.duration")
    .distributionStatisticExpiry(ttl.toJava)
    .publishPercentiles(0.25, 0.5, 0.6, 0.75, 0.8, 0.9, 0.95, 0.97, 0.99)
    .register(meterRegistry)

  override def handle(response: RequestChannel.SendResponse): Unit =
    timer.record(new Runnable() { def run(): Unit = handlerChain.foreach(h => h.handle(response)) })

  override def close(): Unit = meterRegistry.remove(timer)

}