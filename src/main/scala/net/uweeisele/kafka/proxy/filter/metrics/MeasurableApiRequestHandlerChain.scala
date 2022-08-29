package net.uweeisele.kafka.proxy.filter.metrics

import io.micrometer.core.instrument.{MeterRegistry, Timer}
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.request.ApiRequestHandler

import java.io.Closeable
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.language.implicitConversions

object MeasurableApiRequestHandlerChain {
  def apply(handlerChain: Seq[ApiRequestHandler],
            prefix: String,
            ttl: FiniteDuration = Duration(5, MINUTES))
           (implicit meterRegistry: MeterRegistry): MeasurableApiRequestHandlerChain =
    new MeasurableApiRequestHandlerChain(handlerChain, meterRegistry, prefix, ttl)
}

class MeasurableApiRequestHandlerChain(handlerChain: Seq[ApiRequestHandler],
                                       meterRegistry: MeterRegistry,
                                       prefix: String,
                                       ttl: FiniteDuration = Duration(5, MINUTES))
  extends ApiRequestHandler with Closeable {

  private val timer: Timer = Timer.builder(s"$prefix.requesthandler.duration")
    .distributionStatisticExpiry(ttl.toJava)
    .publishPercentiles(0.25, 0.5, 0.6, 0.75, 0.8, 0.9, 0.95, 0.97, 0.99)
    .register(meterRegistry)

  override def handle(request: RequestChannel.Request): Unit =
    timer.record(new Runnable() { def run(): Unit = handlerChain.foreach(h => h.handle(request)) })

  override def close(): Unit = meterRegistry.remove(timer)

}
