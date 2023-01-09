package net.uweeisele.kafka.proxy.filter.metrics

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument._
import io.micrometer.core.instrument.binder.BaseUnits
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.network.RequestChannel.SendResponse
import net.uweeisele.kafka.proxy.request.ApiRequestHandler
import net.uweeisele.kafka.proxy.response.ApiResponseHandler
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, OffsetFetchRequest, ProduceRequest}

import java.io.Closeable
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS, MINUTES}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.language.postfixOps

object ClientMetricsFilter {
  def apply(prefix: String = "kafka.client",
            ttl: FiniteDuration = Duration(5, MINUTES))
           (implicit meterRegistry: MeterRegistry): ClientMetricsFilter =
    new ClientMetricsFilter(prefix, ttl)
}

class ClientMetricsFilter(prefix: String = "kafka",
                          ttl: FiniteDuration = Duration(5, MINUTES))(implicit meterRegistry: MeterRegistry)
  extends ApiRequestHandler with ApiResponseHandler with Closeable with Evictable with LazyLogging {

  private val metricsName = s"$prefix.requests"

  private val baseTagsBuilder: Function[SendResponse, Tags] = (response: SendResponse) => Tags.of(
    Tag.of("clientAddress", response.request.context.clientSocketAddress.getAddress.getHostAddress),
    Tag.of("clientPort", response.request.context.clientSocketAddress.getPort.toString),
    Tag.of("clientId", response.request.context.clientId),
    Tag.of("principal", response.request.context.principal.getName),
    Tag.of("softwareName", response.request.context.clientInformation.softwareName),
    Tag.of("softwareVersion", response.request.context.clientInformation.softwareVersion),
    Tag.of("exposeListenerName", response.request.context.listenerName.value),
    Tag.of("targetListenerName", response.responseContext.listenerName.value),
    Tag.of("apiKey", response.response.apiKey.id.toString),
    Tag.of("apiName", response.response.apiKey.name),
    Tag.of("apiVersion", response.request.context.apiVersion.toString))

  private val contextTagsBuilder: Function[SendResponse, Tags] = (response: SendResponse) => response.response.apiKey match {
    case ApiKeys.PRODUCE => Tags.of(
      Tag.of("acks", response.request.body[ProduceRequest].acks.toString)
    )
    case ApiKeys.FETCH => Tags.of(
      Tag.of("follower", response.request.body[FetchRequest].isFromFollower.toString),
      Tag.of("replicaId", response.request.body[FetchRequest].replicaId.toString),
      Tag.of("rackId", response.request.body[FetchRequest].rackId),
      Tag.of("isolationLevel", response.request.body[FetchRequest].isolationLevel.name),
      Tag.of("maxWait", response.request.body[FetchRequest].maxWait.toString),
      Tag.of("minBytes", response.request.body[FetchRequest].minBytes.toString)
    )
    case ApiKeys.OFFSET_FETCH => Tags.of(
      Tag.of("groupId", response.request.body[OffsetFetchRequest].groupId())
    )
    case _ => Tags.empty()
  }

  private val tagsBuilder: Function[SendResponse, Tags] = (response: SendResponse) => baseTagsBuilder.apply(response).and(contextTagsBuilder.apply(response))

  private val percentiles = Seq(0.5, 0.8, 0.9, 0.95, 0.97, 0.99)

  private val sensors = new EvictableContainer

  private val requestsCounter = sensors.add(EvictableSensor((tags: Tags, apiKey: String) => Counter.builder(s"${metricsName}.${apiKey}").tags(tags).register, ttl))
  private val requestsDurationDistribution = sensors.add(EvictableSensor((tags: Tags, apiKey: String) =>
    Timer.builder(s"${metricsName}.${apiKey}.duration").tags(tags)
      .distributionStatisticExpiry(ttl.toJava)
      .publishPercentiles(percentiles: _*)
      .register, ttl))
  private val requestsErrorsCounter = sensors.add(EvictableSensor((tags: Tags, apiKey: String) => Counter.builder(s"${metricsName}.${apiKey}.errors").tags(tags).register, ttl))

  private val requestsTopicBytesDistribution = sensors.add(EvictableSensor((tags: Tags, apiKey: String) =>
    DistributionSummary.builder(s"${metricsName}.${apiKey}.topic.distribution").tags(tags)
      .distributionStatisticExpiry(ttl.toJava)
      .publishPercentiles(percentiles: _*)
      .baseUnit(BaseUnits.BYTES)
      .register, ttl))

  // private val produceAcks = sensors.add(EvictableSensor(SettableGaugeBuilder((apiKey: String) => s"${metricsName}.${apiKey}.acks"), ttl))

  // private val fetchMaxWait = sensors.add(EvictableSensor(SettableGaugeBuilder((apiKey: String) => s"${metricsName}.${apiKey}.maxWait", b => b.baseUnit(BaseUnits.MILLISECONDS)), ttl))
  // private val fetchMinBytes = sensors.add(EvictableSensor(SettableGaugeBuilder((apiKey: String) => s"${metricsName}.${apiKey}.minBytes", b => b.baseUnit(BaseUnits.BYTES)), ttl))

  override def handle(request: RequestChannel.Request): Unit = {
    request.context.variables(s"${getClass.getName}:${metricsName}:startTimeMs") = System.currentTimeMillis
  }

  override def handle(response: RequestChannel.SendResponse): Unit = {
    val apiKey = response.response.apiKey()
    val tags = tagsBuilder(response)

    requestsCounter(tags, apiKey.name).increment()
    requestsDurationDistribution(tags, apiKey.name).record(measureDuration(response.request).toJava)
    response.response.errorCounts.asScala.foreach {
      case (Errors.NONE, _) => None
      case (error, count) =>
        val errorTags = tags.and(Tag.of("error", error.name))
        requestsErrorsCounter(errorTags, apiKey.name).increment(count.toDouble)
    }

    response.response.apiKey match {
      case ApiKeys.PRODUCE =>
        // produceAcks(tags, apiKey.name).set(response.request.body[ProduceRequest].acks)
        response.request.body[ProduceRequest].data().topicData().asScala.foreach { topicData =>
          val topicTags = tags.and(Tag.of("topic", topicData.name))
          requestsTopicBytesDistribution(topicTags, apiKey.name).record(topicData.partitionData().asScala.map(data => data.records().sizeInBytes()).sum)
        }
      case ApiKeys.FETCH =>
        // fetchMaxWait(tags, apiKey.name).set(response.request.body[FetchRequest].maxWait)
        // fetchMinBytes(tags, apiKey.name).set(response.request.body[FetchRequest].minBytes)
        response.body[FetchResponse].data().responses().asScala.foreach { topicData =>
          val topicTags = tags.and(Tag.of("topic", topicData.topic))
          requestsTopicBytesDistribution(topicTags, apiKey.name).record(topicData.partitions().asScala.map(data => data.records().sizeInBytes()).sum)
        }
      case _ =>
    }
  }

  private def measureDuration(request: RequestChannel.Request): FiniteDuration = {
    request.context.variables.get(s"${getClass.getName}:${metricsName}:startTimeMs") match {
      case Some(startMs: Long) => Duration(System.currentTimeMillis - startMs, MILLISECONDS)
      case _ =>
        logger.warn(s"Request does not contain variable '${getClass.getName}:${metricsName}:startTimeMs'.")
        Duration.Zero
    }
  }

  override def evict(): Unit = sensors.evict()

  override def close(): Unit = sensors.close()

}
