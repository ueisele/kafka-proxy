package net.uweeisele.kafka.proxy.forward
import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.config.{Endpoint, KafkaProxyConfig}
import net.uweeisele.kafka.proxy.network.RequestChannel.CloseConnectionResponse
import net.uweeisele.kafka.proxy.network.{ConnectionListener, RequestChannel}
import net.uweeisele.kafka.proxy.request.ApiRequestHandler
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}

import java.net.InetSocketAddress
import scala.collection.Seq
import scala.collection.mutable.ListBuffer

class RequestForwarder(val config: KafkaProxyConfig,
                       val routeTable: RouteTable,
                       val time: Time) extends ApiRequestHandler with ConnectionListener with LazyLogging {

  private val logContext = new LogContext(s"[RequestForwarder] ")

  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, null) else MemoryPool.NONE

  private val processors = ListBuffer[Processor]()
  private val maxQueuedResponses= config.queuedMaxResponses
  val forwardChannel = new ForwardChannel(maxQueuedResponses)

  private var nextProcessorId = 0
  private var startedProcessingRequests = false
  private var stoppedProcessingRequests = false

  override def handle(request: RequestChannel.Request): Unit =
    routeTable.targetGroupByListener(request.context.listenerNameRef) match {
      case Some(target) => forwardChannel.sendRequest(request, target, request.context.connectionId)
      case None => forwardChannel.sendResponse(new CloseConnectionResponse(request))
    }

  override def onConnectionEstablished(listener: ListenerName, connectionId: String, address: InetSocketAddress): Unit = {
    logger.info(s"Client established connection $connectionId to ${listener.value}://${address.getAddress.getHostAddress}:${address.getPort}")
    routeTable.targetEndpointByConnection(listener, address) match {
      case Some(target) => forwardChannel.connect(target.listenerName, connectionId, new InetSocketAddress(target.host, target.port))
      case None => logger.info(s"Cannot connect to target. No target endpoint found for ${listener.value}://${address.getAddress.getHostAddress}:${address.getPort}")
    }
  }

  override def onConnectionClosed(listener: ListenerName, connectionId: String): Unit = {
    logger.info(s"Client disconnected connection $connectionId at ${listener.value}")
    routeTable.targetGroupByListener(listener) match {
      case Some(target) => forwardChannel.disconnect(target, connectionId)
      case None =>
    }
  }

  def startup(startProcessingRequests: Boolean = true): Unit = {
    this.synchronized {
      createProcessors(config.targets, config.numForwarderThreads)
      if (startProcessingRequests) {
        this.startProcessingRequests()
      }
    }
  }

  def startProcessingRequests(): Unit = {
    logger.info("Starting forwarder and processors")
    this.synchronized {
      if (!startedProcessingRequests) {
        startProcessors()
        startedProcessingRequests = true
      } else {
        logger.info("Forwarder processors already started")
      }
    }
    logger.info("Started forwarder processors")
  }

  private def createProcessors(endpoints: Seq[Endpoint], processorsPerEndpoint: Int): Unit = {
    endpoints.foreach { endpoint =>
      addProcessors(endpoint, processorsPerEndpoint)
      logger.info(s"Created processors for endpoint : ${endpoint.listenerName}")
    }
  }

  private def addProcessors(endpoint: Endpoint, processorsPerEndpoint: Int): Unit = {
    val clusterName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    for (_ <- 0 until processorsPerEndpoint) {
      val processor = new Processor(nextProcessorId, forwardChannel, clusterName, securityProtocol,
        config, time, config.socketRequestMaxBytes, config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs,
        memoryPool, logContext, config.socketSendBufferBytes, config.socketReceiveBufferBytes, config.requestTimeoutMs)
      processors += processor
      forwardChannel.addProcessor(processor)
      nextProcessorId += 1
    }
  }

  private def startProcessors(): Unit = {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(
        s"kafka-proxy-forwarder-thread-${processor.listenerName}-${processor.securityProtocol}-${processor.id}",
        processor
      ).start()
      processor.awaitStartup()
      logger.info(s"Started processor: ${processor.listenerName}-${processor.securityProtocol}-${processor.id}")
    }
  }

  def shutdown(): Unit = {
    logger.info("Shutting down forwarder")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      forwardChannel.shutdown()
    }
    logger.info("Shutdown completed")
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = {
    logger.info("Stopping forwarder request processors")
    this.synchronized {
      processors.foreach(_.initiateShutdown())
      processors.foreach(_.awaitShutdown())
      forwardChannel.clear()
      stoppedProcessingRequests = true
    }
    logger.info("Stopped forward request processors")
  }
}
