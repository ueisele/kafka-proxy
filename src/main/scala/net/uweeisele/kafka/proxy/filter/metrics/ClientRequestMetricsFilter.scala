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

trait Evictable {
  def evict(): Unit
}

class ClientRequestMetricsFilter(meterRegistry: MeterRegistry,
                                 prefix: String = "kafka",
                                 ttl: Duration = (5, MINUTES))
  extends RequestFilter with ResponseFilter with Evictable with Closeable with LazyLogging {

  private val requestApiCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ApiKeys, ListenerName), (Counter, AtomicReference[Instant])] = mutable.Map()
  private val requestSoftwareCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ClientInformation, ListenerName), (Counter, AtomicReference[Instant])] = mutable.Map()

  private val responseApiCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ApiKeys, ListenerName, ListenerName), (Counter, AtomicReference[Instant])] = mutable.Map()
  private val responseApiDurations: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ApiKeys, ListenerName, ListenerName), (Timer, AtomicReference[Instant])] = mutable.Map()

  override def handle(request: RequestChannel.Request): Unit = {
    request.context.variables(s"${getClass.getName}:responses.duration") = System.currentTimeMillis

    val (apiCounter, apiCounterLastUpdated) = requestApiCounters.getOrElseUpdate(
      (request.context.clientSocketAddress, request.context.clientId(), request.context.principal, request.header.apiKey, request.context.listenerNameRef),
      (Counter.builder(s"$prefix.client.requests.api")
        .tag("clientAddress", request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", request.context.clientId())
        .tag("principal", request.context.principal.getName)
        .tag("apiKey", request.header.apiKey.id.toString)
        .tag("apiName", request.header.apiKey.name)
        .tag("apiVersion", request.header.apiVersion.toString)
        .tag("exposeListenerName", request.context.listenerNameRef.value)
        .register(meterRegistry), new AtomicReference(Instant.now)))
    apiCounter.increment()
    apiCounterLastUpdated.set(Instant.now)

    val (softwareCounter, softwareCounterLastUpdated) = requestSoftwareCounters.getOrElseUpdate(
      (request.context.clientSocketAddress, request.context.clientId(), request.context.principal, request.context.clientInformation, request.context.listenerNameRef),
      (Counter.builder(s"$prefix.clients.requests.software")
        .tag("clientAddress", request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", request.context.clientId())
        .tag("principal", request.context.principal.getName)
        .tag("softwareName", request.context.clientInformation.softwareName)
        .tag("softwareVersion", request.context.clientInformation.softwareVersion)
        .tag("exposeListenerName", request.context.listenerNameRef.value)
        .register(meterRegistry), new AtomicReference(Instant.now)))
    softwareCounter.increment()
    softwareCounterLastUpdated.set(Instant.now)
  }

  override def handle(response: RequestChannel.SendResponse): Unit = {
    val (apiCounter, apiCounterLastUpdated) = responseApiCounters.getOrElseUpdate(
      (response.request.context.clientSocketAddress, response.request.context.clientId(), response.request.context.principal, response.response.apiKey, response.request.context.listenerNameRef, response.forwardContext.listenerNameRef),
      (Counter.builder(s"$prefix.client.responses.api")
        .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", response.request.context.clientId())
        .tag("principal", response.request.context.principal.getName)
        .tag("apiKey", response.response.apiKey.id.toString)
        .tag("apiName", response.response.apiKey.name)
        .tag("apiVersion", response.forwardContext.apiVersion.toString)
        .tag("exposeListenerName", response.request.context.listenerNameRef.value)
        .tag("targetListenerName", response.forwardContext.listenerNameRef.value)
        .register(meterRegistry), new AtomicReference(Instant.now)))
    apiCounter.increment()
    apiCounterLastUpdated.set(Instant.now)

    val (apiTimer, apiTimerLastUpdated) = responseApiDurations.getOrElseUpdate(
      (response.request.context.clientSocketAddress, response.request.context.clientId(), response.request.context.principal, response.response.apiKey, response.request.context.listenerNameRef, response.forwardContext.listenerNameRef),
      (Timer.builder(s"$prefix.client.responses.api.duration")
        .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
        .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
        .tag("clientId", response.request.context.clientId())
        .tag("principal", response.request.context.principal.getName)
        .tag("apiKey", response.response.apiKey.id.toString)
        .tag("apiName", response.response.apiKey.name)
        .tag("apiVersion", response.forwardContext.apiVersion.toString)
        .tag("exposeListenerName", response.request.context.listenerNameRef.value)
        .tag("targetListenerName", response.forwardContext.listenerNameRef.value)
        .distributionStatisticExpiry(Duration.create(5, MINUTES).toJava)
        .publishPercentiles(0.25, 0.5, 0.6, 0.75, 0.8, 0.9, 0.95, 0.97, 0.99)
        .register(meterRegistry), new AtomicReference(Instant.now)))
    apiTimer.record(measureDuration(response.request).toJava)
    apiTimerLastUpdated.set(Instant.now)
  }

  private def measureDuration(request: RequestChannel.Request): FiniteDuration = {
    request.context.variables.get(s"${getClass.getName}:responses.duration") match {
      case Some(startMs: Long) => (System.currentTimeMillis - startMs, MILLISECONDS)
      case _ =>
        logger.warn(s"Something went wrong! Request does not contain variable '${this.getClass.getName}:responses.duration'.")
        Duration.Zero
    }
  }

  override def evict(): Unit = {
    val evictBefore = Instant.now.minusMillis(ttl.toMillis)
    evict(requestApiCounters, evictBefore)
    evict(requestSoftwareCounters, evictBefore)
    evict(responseApiCounters, evictBefore)
    evict(responseApiDurations, evictBefore)
  }

  private def evict[K,M](evictableMap: mutable.Map[K, (M, AtomicReference[Instant])], evictBefore: Instant): Unit = {
    evictableMap.filterInPlace { case (_, (_, lastUpdated)) =>
      lastUpdated.get().isAfter(evictBefore)
    }
  }

  override def close(): Unit = {
    close(requestApiCounters)
    close(requestSoftwareCounters)
    close(responseApiCounters)
    close(responseApiDurations)
  }

  private def close[K,M<:Meter,T](meterMap: mutable.Map[K, (M, T)]): Unit = {
    meterMap.foreach { case (_, (meter: Meter, _)) =>
      meterRegistry.remove(meter)
    }
    meterMap.clear()
  }

}
