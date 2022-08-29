package net.uweeisele.kafka.proxy.filter.metrics

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.binder.BaseUnits.BYTES
import io.micrometer.core.instrument.{Counter, Meter, MeterRegistry, Timer}
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.request.ApiRequestHandler
import net.uweeisele.kafka.proxy.response.ApiResponseHandler
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.common.security.auth.KafkaPrincipal

import java.io.Closeable
import java.net.InetSocketAddress
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, MINUTES}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.language.postfixOps

object ProduceClientApiMetricsFilter {
  def apply(prefix: String = "kafka",
            ttl: FiniteDuration = Duration(5, MINUTES))
           (implicit meterRegistry: MeterRegistry): ProduceClientApiMetricsFilter =
    new ProduceClientApiMetricsFilter(meterRegistry, prefix, ttl)
}

class ProduceClientApiMetricsFilter(meterRegistry: MeterRegistry,
                                    prefix: String = "kafka",
                                    ttl: FiniteDuration = Duration(5, MINUTES))
  extends ApiRequestHandler with ApiResponseHandler with Closeable with Evictable with LazyLogging {

  private val produceRequestCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ListenerName, Short), (Counter, AtomicReference[Instant])] = mutable.Map()
  private val produceRequestTopicCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ListenerName, Short, String), (Counter, AtomicReference[Instant])] = mutable.Map()
  private val produceRequestTopicBytesCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ListenerName, Short, String), (Counter, AtomicReference[Instant])] = mutable.Map()

  private val produceResponseCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ListenerName, ListenerName, Short), (Counter, AtomicReference[Instant])] = mutable.Map()
  private val produceResponseDurations: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ListenerName, ListenerName, Short), (Timer, AtomicReference[Instant])] = mutable.Map()
  private val produceResponseTopicErrorsCounters: mutable.Map[(InetSocketAddress, String, KafkaPrincipal, ListenerName, ListenerName, Short, String, Errors), (Counter, AtomicReference[Instant])] = mutable.Map()

  override def handle(request: RequestChannel.Request): Unit = {
    request.context.apiKey match {
      case ApiKeys.PRODUCE =>
        val produceRequest: ProduceRequest = request.body[ProduceRequest]
        request.context.variables(s"${getClass.getName}:$prefix.requests.produce.duration") = System.currentTimeMillis
        request.context.variables(s"${getClass.getName}:$prefix.requests.produce.acks") = produceRequest.acks

        val (countersMap, countersMapLastUpdated) = produceRequestCounters.getOrElseUpdate(
          (request.context.clientSocketAddress, request.context.clientId, request.context.principal, request.context.listenerName, produceRequest.acks),
          (Counter.builder(s"$prefix.requests.produce")
            .tag("clientAddress", request.context.clientSocketAddress.getAddress.getHostAddress)
            .tag("clientPort", request.context.clientSocketAddress.getPort.toString)
            .tag("clientId", request.context.clientId)
            .tag("principal", request.context.principal.getName)
            .tag("exposeListenerName", request.context.listenerName.value)
            .tag("apiVersion", request.context.apiVersion.toString)
            .tag("acks", produceRequest.acks.toString)
            .register(meterRegistry), new AtomicReference(Instant.now)))
        countersMapLastUpdated.set(Instant.now)
        countersMap.increment()

        produceRequest.data().topicData().asScala.foreach { topicData =>
          val (topicCountersMap, topicCountersMapLastUpdated) = produceRequestTopicCounters.getOrElseUpdate(
            (request.context.clientSocketAddress, request.context.clientId, request.context.principal, request.context.listenerName, produceRequest.acks, topicData.name),
            (Counter.builder(s"$prefix.requests.produce.topic")
              .tag("clientAddress", request.context.clientSocketAddress.getAddress.getHostAddress)
              .tag("clientPort", request.context.clientSocketAddress.getPort.toString)
              .tag("clientId", request.context.clientId)
              .tag("principal", request.context.principal.getName)
              .tag("exposeListenerName", request.context.listenerName.value)
              .tag("apiVersion", request.context.apiVersion.toString)
              .tag("acks", produceRequest.acks.toString)
              .tag("topic", topicData.name)
              .register(meterRegistry), new AtomicReference(Instant.now)))
          topicCountersMapLastUpdated.set(Instant.now)
          topicCountersMap.increment()

          val (topicBytesCountersMap, topicBytesCountersMapLastUpdated) = produceRequestTopicBytesCounters.getOrElseUpdate(
            (request.context.clientSocketAddress, request.context.clientId, request.context.principal, request.context.listenerName, produceRequest.acks, topicData.name),
            (Counter.builder(s"$prefix.requests.produce.topic.bytes")
              .tag("clientAddress", request.context.clientSocketAddress.getAddress.getHostAddress)
              .tag("clientPort", request.context.clientSocketAddress.getPort.toString)
              .tag("clientId", request.context.clientId)
              .tag("principal", request.context.principal.getName)
              .tag("exposeListenerName", request.context.listenerName.value)
              .tag("apiVersion", request.context.apiVersion.toString)
              .tag("acks", produceRequest.acks.toString)
              .tag("topic", topicData.name)
              .baseUnit(BYTES)
              .register(meterRegistry), new AtomicReference(Instant.now)))
          topicBytesCountersMapLastUpdated.set(Instant.now)
          topicBytesCountersMap.increment(topicData.partitionData().asScala.map(data => data.records().sizeInBytes()).sum)
        }
      case _ =>
    }
  }

  override def handle(response: RequestChannel.SendResponse): Unit = {
    response.response.apiKey match {
      case ApiKeys.PRODUCE =>
        val (countersMap, countersMapLastUpdated) = produceResponseCounters.getOrElseUpdate(
          (response.request.context.clientSocketAddress, response.request.context.clientId, response.request.context.principal,
            response.request.context.listenerName, response.responseContext.listenerName, acks(response.request)),
          (Counter.builder(s"$prefix.responses.produce")
            .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
            .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
            .tag("clientId", response.request.context.clientId)
            .tag("principal", response.request.context.principal.getName)
            .tag("exposeListenerName", response.request.context.listenerName.value)
            .tag("targetListenerName", response.responseContext.listenerName.value)
            .tag("apiVersion", response.request.context.apiVersion.toString)
            .tag("acks", acks(response.request).toString)
            .register(meterRegistry), new AtomicReference(Instant.now)))
        countersMapLastUpdated.set(Instant.now)
        countersMap.increment()

        val (durationsMap, durationsMapLastUpdated) = produceResponseDurations.getOrElseUpdate(
          (response.request.context.clientSocketAddress, response.request.context.clientId, response.request.context.principal,
            response.request.context.listenerName, response.responseContext.listenerName, acks(response.request)),
          (Timer.builder(s"$prefix.responses.produce.duration")
            .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
            .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
            .tag("clientId", response.request.context.clientId)
            .tag("principal", response.request.context.principal.getName)
            .tag("exposeListenerName", response.request.context.listenerName.value)
            .tag("targetListenerName", response.responseContext.listenerName.value)
            .tag("apiVersion", response.request.context.apiVersion.toString)
            .tag("acks", acks(response.request).toString)
            .distributionStatisticExpiry(ttl.toJava)
            .publishPercentiles(0.25, 0.5, 0.6, 0.75, 0.8, 0.9, 0.95, 0.97, 0.99)
            .register(meterRegistry), new AtomicReference(Instant.now)))
        durationsMapLastUpdated.set(Instant.now)
        durationsMap.record(measureDuration(response.request).toJava)

        val produceResponse: ProduceResponse = response.response.asInstanceOf[ProduceResponse]
        produceResponse.data().responses().asScala.foreach { topicData =>
          topicData.partitionResponses().asScala.map(p => Errors.forCode(p.errorCode())).groupBy(e => e).map(m => (m._1, m._2.size)).foreach {
            case (Errors.NONE, _) => None
            case (error, count) =>
              val (topicErrorCountersMap, topicErrorCountersMapLastUpdated) = produceResponseTopicErrorsCounters.getOrElseUpdate(
                (response.request.context.clientSocketAddress, response.request.context.clientId, response.request.context.principal,
                  response.request.context.listenerName, response.responseContext.listenerName, acks(response.request), topicData.name, error),
                (Counter.builder(s"$prefix.responses.produce.topic.error")
                  .tag("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress)
                  .tag("clientPort", response.request.context.clientSocketAddress.getPort.toString)
                  .tag("clientId", response.request.context.clientId)
                  .tag("principal", response.request.context.principal.getName)
                  .tag("exposeListenerName", response.request.context.listenerName.value)
                  .tag("targetListenerName", response.responseContext.listenerName.value)
                  .tag("apiVersion", response.request.context.apiVersion.toString)
                  .tag("acks", acks(response.request).toString)
                  .tag("topic", topicData.name)
                  .tag("error", error.name)
                  .register(meterRegistry), new AtomicReference(Instant.now)))
              topicErrorCountersMapLastUpdated.set(Instant.now)
              topicErrorCountersMap.increment(count)
          }
        }
      case _ =>
    }
  }

  private def acks(request: RequestChannel.Request): Short = {
    request.context.variables.get(s"${getClass.getName}:$prefix.requests.produce.acks") match {
      case Some(acks: Short) =>acks
      case _ =>
        logger.warn(s"Something went wrong! Request does not contain variable '${getClass.getName}:$prefix.requests.produce.acks'.")
        -2
    }
  }

  private def measureDuration(request: RequestChannel.Request): FiniteDuration = {
    request.context.variables.get(s"${getClass.getName}:$prefix.requests.produce.duration") match {
      case Some(startMs: Long) => (System.currentTimeMillis - startMs, MILLISECONDS)
      case _ =>
        logger.warn(s"Something went wrong! Request does not contain variable '${getClass.getName}:$prefix.requests.produce.duration'.")
        Duration.Zero
    }
  }

  override def evict(): Unit = {
    val evictBefore = Instant.now.minusMillis(ttl.toMillis)
    evict(produceRequestCounters, evictBefore)
    evict(produceRequestTopicCounters, evictBefore)
  }

  private def evict[K,M<:Meter](evictableMap: mutable.Map[K, (M, AtomicReference[Instant])], evictBefore: Instant): Unit = {
    evictableMap.filterInPlace { case (_, (meter, lastUpdated)) =>
      if (lastUpdated.get().isBefore(evictBefore)) {
        meterRegistry.remove(meter)
        false
      } else {
        true
      }
    }
  }

  override def close(): Unit = {
    close(produceRequestCounters)
    close(produceRequestTopicCounters)
  }

  private def close[K,M<:Meter,T](meterMap: mutable.Map[K, (M, T)]): Unit = {
    meterMap.foreach { case (_, (meter, _)) =>
      meterRegistry.remove(meter)
    }
    meterMap.clear()
  }

}
