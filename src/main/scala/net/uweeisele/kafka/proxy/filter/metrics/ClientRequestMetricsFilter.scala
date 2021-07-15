package net.uweeisele.kafka.proxy.filter.metrics

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{Counter, Meter, MeterRegistry, Timer}
import net.uweeisele.kafka.proxy.filter.{RequestFilter, ResponseFilter}
import net.uweeisele.kafka.proxy.network.RequestChannel
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.security.auth.KafkaPrincipal

import java.io.Closeable
import java.net.InetSocketAddress
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, MINUTES}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.language.postfixOps

class ClientRequestMetricsFilter(meterRegistry: MeterRegistry,
                                 prefix: String = "kafka",
                                 ttl: FiniteDuration = Duration(5, MINUTES))
  extends RequestFilter with ResponseFilter with Closeable with LazyLogging {

  private val requestApiCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ClientInformation, ListenerName), (mutable.Map[ApiKeys, Counter], AtomicReference[Instant])] = mutable.Map()

  private val responseApiCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ClientInformation, ListenerName, ListenerName), (mutable.Map[ApiKeys, Counter], AtomicReference[Instant])]= mutable.Map()
  private val responseApiDurations: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ClientInformation, ListenerName, ListenerName), (mutable.Map[ApiKeys, Timer], AtomicReference[Instant])] = mutable.Map()

  override def handle(request: RequestChannel.Request): Unit = {
    request.context.variables(s"${getClass.getName}:$prefix.client.responses.api.duration") = System.currentTimeMillis

    val (apiCountersMap, apiCountersMapLastUpdated) = requestApiCounters.getOrElseUpdate(
      (request.context.clientSocketAddress, request.context.clientId, request.context.principal, request.context.clientInformation, request.context.listenerName),
      (mutable.Map(), new AtomicReference(Instant.now)))
    apiCountersMapLastUpdated.set(Instant.now)
    apiCountersMap.getOrElseUpdate(request.context.apiKey,
      Counter.builder(s"$prefix.client.requests.api")
        .tag("clientAddress", request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", request.context.clientId)
        .tag("principal", request.context.principal.getName)
        .tag("softwareName", request.context.clientInformation.softwareName)
        .tag("softwareVersion", request.context.clientInformation.softwareVersion)
        .tag("exposeListenerName", request.context.listenerName.value)
        .tag("apiKey", request.context.apiKey.id.toString)
        .tag("apiName", request.context.apiKey.name)
        .tag("apiVersion", request.context.apiVersion.toString)
        .register(meterRegistry))
      .increment()
  }

  override def handle(response: RequestChannel.SendResponse): Unit = {
    val (apiCountersMap, apiCountersMapLastUpdated) = responseApiCounters.getOrElseUpdate(
      (response.request.context.clientSocketAddress, response.request.context.clientId, response.request.context.principal,
        response.request.context.clientInformation, response.request.context.listenerName, response.responseContext.listenerName),
      (mutable.Map(), new AtomicReference(Instant.now)))
    apiCountersMapLastUpdated.set(Instant.now)
    apiCountersMap.getOrElseUpdate(response.response.apiKey,
      Counter.builder(s"$prefix.client.responses.api")
        .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", response.request.context.clientId)
        .tag("principal", response.request.context.principal.getName)
        .tag("softwareName", response.request.context.clientInformation.softwareName)
        .tag("softwareVersion", response.request.context.clientInformation.softwareVersion)
        .tag("exposeListenerName", response.request.context.listenerName.value)
        .tag("targetListenerName", response.responseContext.listenerName.value)
        .tag("apiKey", response.response.apiKey.id.toString)
        .tag("apiName", response.response.apiKey.name)
        .tag("apiVersion", response.request.context.apiVersion.toString)
        .register(meterRegistry))
      .increment()

    val (apiDurationsMap, apiDurationsMapLastUpdated) = responseApiDurations.getOrElseUpdate(
      (response.request.context.clientSocketAddress, response.request.context.clientId, response.request.context.principal,
        response.request.context.clientInformation, response.request.context.listenerName, response.responseContext.listenerName),
      (mutable.Map(), new AtomicReference(Instant.now)))
    apiDurationsMapLastUpdated.set(Instant.now)
    apiDurationsMap.getOrElseUpdate(response.response.apiKey,
      Timer.builder(s"$prefix.client.responses.api.duration")
        .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", response.request.context.clientId)
        .tag("principal", response.request.context.principal.getName)
        .tag("softwareName", response.request.context.clientInformation.softwareName)
        .tag("softwareVersion", response.request.context.clientInformation.softwareVersion)
        .tag("exposeListenerName", response.request.context.listenerName.value)
        .tag("targetListenerName", response.responseContext.listenerName.value)
        .tag("apiKey", response.response.apiKey.id.toString)
        .tag("apiName", response.response.apiKey.name)
        .tag("apiVersion", response.request.context.apiVersion.toString)
        .distributionStatisticExpiry(ttl.toJava)
        .publishPercentiles(0.25, 0.5, 0.6, 0.75, 0.8, 0.9, 0.95, 0.97, 0.99)
        .register(meterRegistry))
      .record(measureDuration(response.request).toJava)
  }

  private def measureDuration(request: RequestChannel.Request): FiniteDuration = {
    request.context.variables.get(s"${getClass.getName}:$prefix.client.responses.api.duration") match {
      case Some(startMs: Long) => (System.currentTimeMillis - startMs, MILLISECONDS)
      case _ =>
        logger.warn(s"Something went wrong! Request does not contain variable '${this.getClass.getName}:responses.duration'.")
        Duration.Zero
    }
  }

  def evict(): Unit = {
    val evictBefore = Instant.now.minusMillis(ttl.toMillis)
    evict(requestApiCounters, evictBefore)
    evict(responseApiCounters, evictBefore)
    evict(responseApiDurations, evictBefore)
  }

  private def evict[K,M<:Meter](evictableMap: mutable.Map[K, (mutable.Map[ApiKeys,M], AtomicReference[Instant])], evictBefore: Instant): Unit = {
    evictableMap.filterInPlace { case (_, (meterMap, lastUpdated)) =>
      if (lastUpdated.get().isBefore(evictBefore)) {
        meterMap.values.foreach(meterRegistry.remove)
        false
      } else {
        true
      }
    }
  }

  override def close(): Unit = {
    close(requestApiCounters)
    close(responseApiCounters)
    close(responseApiDurations)
  }

  private def close[K,M<:Meter,T](meterMap: mutable.Map[K, (mutable.Map[ApiKeys,M], T)]): Unit = {
    meterMap.foreach { case (_, (meterMap, _)) =>
      meterMap.values.foreach(meterRegistry.remove)
    }
    meterMap.clear()
  }

}
