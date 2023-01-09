package net.uweeisele.kafka.proxy


import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.util.NamedThreadFactory
import io.micrometer.core.instrument.{MeterRegistry, Metrics}
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}
import io.prometheus.client.exporter.HTTPServer
import net.uweeisele.kafka.proxy.config.KafkaProxyConfig
import net.uweeisele.kafka.proxy.filter.advertisedlistener.{AdvertisedListenerRewriteFilter, AdvertisedListenerTable}
import net.uweeisele.kafka.proxy.filter.apiversion.ApiVersionFilter
import net.uweeisele.kafka.proxy.filter.metrics.{ClientMetricsFilter, Evictable, MeasurableApiRequestHandlerChain, MeasurableApiResponseHandlerChain}
import net.uweeisele.kafka.proxy.forward.{RequestForwarder, RouteTable}
import net.uweeisele.kafka.proxy.network.SocketServer
import net.uweeisele.kafka.proxy.request.{ApiRequestHandler, ApiRequestHandlerChain, RequestHandlerPool}
import net.uweeisele.kafka.proxy.response.{ApiResponseHandler, ApiResponseHandlerChain, ResponseHandlerPool}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.utils.Time

import java.io.Closeable
import java.util.Properties
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, NANOSECONDS}

object KafkaProxy {
  def fromProps(serverProps: Properties): KafkaProxy = {
    new KafkaProxy(KafkaProxyConfig.fromProps(serverProps, false))
  }
}

class KafkaProxy(val proxyConfig: KafkaProxyConfig, time: Time = Time.SYSTEM) extends LazyLogging {

  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  private var evictionScheduler: ScheduledExecutorService = null

  private var metricsHttpServer: HTTPServer = null
  private val metricsFilters: ListBuffer[ApiRequestHandler with ApiResponseHandler with Evictable with Closeable] = ListBuffer()

  private var socketServer: SocketServer = null
  private var requestHandlerPool: RequestHandlerPool = null
  private var requestForwarder: RequestForwarder = null
  private var responseHandlerPool: ResponseHandlerPool = null

  def startup(): Unit = {
    try {
      logger.info("starting")

      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka proxy is still shutting down, cannot re-start!")

      if (startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        evictionScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("eviction"))

        val prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.addRegistry(prometheusMeterRegistry)
        implicit val meterRegistry: MeterRegistry = Metrics.globalRegistry

        metricsHttpServer = new HTTPServer.Builder()
          .withHostname(proxyConfig.filterMetricsListenerHostname)
          .withPort(proxyConfig.filterMetricsListenerPort)
          .withRegistry(prometheusMeterRegistry.getPrometheusRegistry)
          .build()

        //metricsFilters += ApiMetricsFilter(proxyConfig.routes.keySet.toSeq, proxyConfig.routes.values.toSet.toSeq)
        metricsFilters += ClientMetricsFilter("kafka.client", proxyConfig.filterMetricsExpiry)
        evictionScheduler.scheduleAtFixedRate(() => metricsFilters.foreach(_.evict()), 1, 1, MINUTES)

        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        socketServer = new SocketServer(proxyConfig, time)
        socketServer.startup(startProcessingRequests = false)

        val routeTable = new RouteTable(proxyConfig.routes, proxyConfig.listeners, proxyConfig.targets)
        requestForwarder = new RequestForwarder(proxyConfig, routeTable, time)
        requestForwarder.startup()
        socketServer.addConnectionListener(requestForwarder)

        val apiRequestHandlerChain = ApiRequestHandlerChain(Seq[ApiRequestHandler](
          MeasurableApiRequestHandlerChain(metricsFilters.toSeq, "metrics"),
          request => { request.header.apiKey match {
            case ApiKeys.FETCH => None
            case ApiKeys.BROKER_HEARTBEAT => None
            case _ => println(request)
          }}, requestForwarder))
        requestHandlerPool = new RequestHandlerPool(socketServer.requestChannel, apiRequestHandlerChain, proxyConfig.numRequestHandlerThreads)
        requestHandlerPool.start()

        val advertisedListenerTable = new AdvertisedListenerTable(proxyConfig.listeners, proxyConfig.advertisedListeners)
        val apiResponseHandlerChain = ApiResponseHandlerChain(Seq[ApiResponseHandler](
          new ApiVersionFilter,
          new AdvertisedListenerRewriteFilter(routeTable, advertisedListenerTable),
          response => { response.request.header.apiKey match {
            case ApiKeys.FETCH => None
            case ApiKeys.BROKER_HEARTBEAT => None
            case _ => println(response)
          }}, MeasurableApiResponseHandlerChain(metricsFilters.toSeq.reverse, "metrics")))
        responseHandlerPool = new ResponseHandlerPool(socketServer.requestChannel, requestForwarder.forwardChannel, apiResponseHandlerChain, proxyConfig.numResponseHandlerThreads)
        responseHandlerPool.start()

        socketServer.startProcessingRequests()

        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        logger.info("started")
      }
    } catch {
      case e: Throwable =>
        logger.error("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  def shutdown(): Unit = {
    try {
      logger.info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null) {
          try {socketServer.stopProcessingRequests()} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        if (requestHandlerPool != null) {
          try {requestHandlerPool.shutdown()} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        if(requestForwarder != null) {
          try {requestForwarder.shutdown()} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        if(responseHandlerPool != null) {
          try {responseHandlerPool.shutdown()} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        if (socketServer != null) {
          try {socketServer.shutdown()} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        if (metricsHttpServer != null) {
          try {metricsHttpServer.close()} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        if (metricsFilters != null) {
          try {metricsFilters.foreach(_.close())} catch { case e: Throwable => logger.error(e.getMessage, e) }
        }

        Metrics.globalRegistry.close()

        if (evictionScheduler != null) {
          evictionScheduler.shutdownNow()
          evictionScheduler.awaitTermination(Long.MaxValue, NANOSECONDS)
        }

        startupComplete.set(false)
        isShuttingDown.set(false)
        shutdownLatch.countDown()
        logger.info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        logger.error("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  def awaitShutdown(): Unit = shutdownLatch.await()

}