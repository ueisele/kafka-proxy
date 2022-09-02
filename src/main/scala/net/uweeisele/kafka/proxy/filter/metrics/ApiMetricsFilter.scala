package net.uweeisele.kafka.proxy.filter.metrics

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{Counter, Meter, MeterRegistry, Timer}
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.request.ApiRequestHandler
import net.uweeisele.kafka.proxy.response.ApiResponseHandler
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys

import java.io.Closeable
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, SECONDS}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.language.postfixOps;

object ApiMetricsFilter {
  def apply(exposeListeners: Seq[ListenerName],
            targetListeners: Seq[ListenerName],
            prefix: String = "kafka")
           (implicit meterRegistry: MeterRegistry): ApiMetricsFilter =
    new ApiMetricsFilter(exposeListeners, targetListeners, meterRegistry, prefix)
}

class ApiMetricsFilter(exposeListeners: Seq[ListenerName],
                       targetListeners: Seq[ListenerName],
                       meterRegistry: MeterRegistry,
                       prefix: String = "kafka")
  extends ApiRequestHandler with ApiResponseHandler with Closeable with LazyLogging {

  private val requestCounters = ApiKeys.values().flatMap(apiKey => exposeListeners.map(listener => (apiKey, listener))).map {
    case (apiKey: ApiKeys, listener: ListenerName) =>
      (apiKey, listener) -> Counter.builder(s"$prefix.requests")
        .tag("apiKey", apiKey.id.toString)
        .tag("apiName", apiKey.name)
        .tag("exposeListenerName", listener.value())
        .register(meterRegistry)
    } toMap

  private val responseCounters = ApiKeys.values().flatMap(apiKey => exposeListeners.flatMap(el => targetListeners.map(tl => (apiKey, el, tl)))).map {
    case (apiKey: ApiKeys, exposedListener: ListenerName, targetListener: ListenerName) =>
      (apiKey, exposedListener, targetListener) -> Counter.builder(s"$prefix.responses")
        .tag("apiKey", apiKey.id.toString)
        .tag("apiName", apiKey.name)
        .tag("exposeListenerName", exposedListener.value())
        .tag("targetListenerName", targetListener.value())
        .register(meterRegistry)
      } toMap

  private val responseDurations = ApiKeys.values().flatMap(apiKey => exposeListeners.flatMap(el => targetListeners.map(tl => (apiKey, el, tl)))).map {
    case (apiKey: ApiKeys, exposedListener: ListenerName, targetListener: ListenerName) =>
      (apiKey, exposedListener, targetListener) -> Timer.builder(s"$prefix.responses.duration")
        .tag("apiKey", apiKey.id.toString)
        .tag("apiName", apiKey.name)
        .tag("exposeListenerName", exposedListener.value())
        .tag("targetListenerName", targetListener.value())
        .distributionStatisticExpiry(Duration.create(300, SECONDS).toJava)
        .publishPercentiles(0.25, 0.5, 0.6, 0.75, 0.8, 0.9, 0.95, 0.97, 0.99)
        .register(meterRegistry)
      } toMap

  override def handle(request: RequestChannel.Request): Unit = {
    request.context.variables(s"${getClass.getName}:$prefix.responses.duration") = System.currentTimeMillis
    requestCounters.get((request.header.apiKey, request.context.listenerName)) match {
      case Some(counter) => counter.increment()
      case None => logger.info(s"ApiKey ${request.header.apiKey.name} or listener ${request.context.listenerName.value} is unknown.")
    }
  }

  override def handle(response: RequestChannel.SendResponse): Unit = {
    responseCounters.get((response.response.apiKey, response.request.context.listenerName, response.responseContext.listenerName)) match {
      case Some(counter) => counter.increment()
      case None => logger.info(
        s"ApiKey ${response.response.apiKey.name} or " +
          s"expose listener ${response.request.context.listenerName.value} " +
          s"or target listener ${response.responseContext.listenerName.value} is unknown.")
    }
    responseDurations.get((response.response.apiKey, response.request.context.listenerName, response.responseContext.listenerName)) match {
      case Some(timer) => timer.record(measureDuration(response.request).toJava)
      case None => logger.info(
        s"ApiKey ${response.response.apiKey.name} or " +
          s"expose listener ${response.request.context.listenerName.value} " +
          s"or target listener ${response.responseContext.listenerName.value} is unknown.")
    }
  }

  private def measureDuration(request: RequestChannel.Request): FiniteDuration = {
    request.context.variables.get(s"${getClass.getName}:$prefix.responses.duration") match {
      case Some(startMs: Long) => Duration(System.currentTimeMillis - startMs, MILLISECONDS)
      case _ =>
        logger.warn(s"Something went wrong! Request does not contain variable '${getClass.getName}:$prefix.responses.duration'.")
        Duration.Zero
    }
  }

  override def close(): Unit = {
    close(requestCounters)
    close(responseCounters)
    close(responseDurations)
  }

  private def close[K,M<:Meter](meterMap: Map[K, M]): Unit = {
    meterMap.foreach { case (_, meter: Meter) =>
      meterRegistry.remove(meter)
    }
  }

}
