package net.uweeisele.kafka.proxy.network

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.cluster.ClusterEndpoint
import net.uweeisele.kafka.proxy.security.CredentialProvider
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}

import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class SocketServer(val config: AbstractConfig,
                   val time: Time,
                   val credentialProvider: CredentialProvider) extends LazyLogging{

  private val logContext = new LogContext(s"[SocketServer] ")

  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, null) else MemoryPool.NONE

  private val processors = new ConcurrentHashMap[Int, Processor]()
  private[network] val acceptors = new ConcurrentHashMap[ClusterEndpoint, Acceptor]()
  private val maxQueuedRequests = config.queuedMaxRequests
  val requestChannel = new RequestChannel(maxQueuedRequests)

  private var nextProcessorId = 0
  private var startedProcessingRequests = false
  private var stoppedProcessingRequests = false

  /**
   * Starts the socket server and creates all the Acceptors and the Processors. The Acceptors
   * start listening at this stage so that the bound port is known when this method completes
   * even when ephemeral ports are used. Acceptors and Processors are started if `startProcessingRequests`
   * is true. If not, acceptors and processors are only started when
   * [[net.uweeisele.kafka.proxy.network.SocketServer#startProcessingRequests()]]
   * is invoked. Delayed starting of acceptors and processors is used to delay processing client
   * connections until server is fully initialized, e.g. to ensure that all credentials have been
   * loaded before authentications are performed. Incoming connections on this server are processed
   * when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startProcessingRequests Flag indicating whether `Processor`s must be started.
   */
  def startup(startProcessingRequests: Boolean = true): Unit = {
    this.synchronized {
      createAcceptorsAndProcessors(config.numNetworkThreads, config.dataPlaneListeners)
      if (startProcessingRequests) {
        this.startProcessingRequests()
      }
    }
  }

  /**
   * Start processing requests and new connections. This method is used for delayed starting of
   * all the acceptors and processors if [[net.uweeisele.kafka.proxy.network.SocketServer#startup]]
   * was invoked with `startProcessingRequests=false`.
   *
   * Before starting processors for each endpoint, we ensure that authorizer has all the metadata
   * to authorize requests on that endpoint by waiting on the provided future. We start inter-broker
   * listener before other listeners. This allows authorization metadata for other listeners to be
   * stored in Kafka topics in this cluster.
   *
   * @param authorizerFutures Future per [[EndPoint]] used to wait before starting the processor
   *                          corresponding to the [[EndPoint]]
   */
  def startProcessingRequests(authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    logger.info("Starting socket server acceptors and processors")
    this.synchronized {
      if (!startedProcessingRequests) {
        startProcessorsAndAcceptors(authorizerFutures)
        startedProcessingRequests = true
      } else {
        logger.info("Socket server acceptors and processors already started")
      }
    }
    logger.info("Started socket server acceptors and processors")
  }

  private def createAcceptorsAndProcessors(clusterEndpoints: Seq[ClusterEndpoint], processorsPerClusterEndpoint: Int): Unit = {
    clusterEndpoints.foreach { clusterEndpoint =>
      val acceptor = createAcceptor(clusterEndpoint)
      addProcessors(acceptor, clusterEndpoint, processorsPerClusterEndpoint)
      acceptors.put(clusterEndpoint, acceptor)
      logger.info(s"Created data-plane acceptor and processors for endpoint : ${clusterEndpoint.clusterName}")
    }
  }

  private def createAcceptor(clusterEndpoint: ClusterEndpoint) : Acceptor = {
    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    new Acceptor(clusterEndpoint, recvBufferSize, sendBufferSize)
  }

  private def addProcessors(acceptor: Acceptor, clusterEndpoint: ClusterEndpoint, processorsPerClusterEndpoint: Int): Unit = {
    val clusterName = clusterEndpoint.clusterName
    val securityProtocol = clusterEndpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()
    for (_ <- 0 until processorsPerClusterEndpoint) {
      val processor = newProcessor(nextProcessorId, requestChannel, clusterName, securityProtocol, memoryPool)
      listenerProcessors += processor
      requestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => processors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors)
  }

  protected[network] def newProcessor(id: Int, requestChannel: RequestChannel, clusterName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      clusterName,
      securityProtocol,
      config,
      credentialProvider,
      memoryPool,
      logContext
    )
  }

  /**
   * Starts processors of all the data-plane acceptors and all the acceptors of this server.
   */
  private def startProcessorsAndAcceptors(): Unit = {
    acceptors.asScala.values.foreach { acceptor =>
      startAcceptorAndProcessors(acceptor.clusterEndpoint, acceptor)
    }
  }

  /**
   * Starts processors of the provided acceptor and the acceptor itself.
   *
   * Before starting them, we ensure that authorizer has all the metadata to authorize
   * requests on that endpoint by waiting on the provided future.
   */
  private def startAcceptorAndProcessors(clusterEndpoint: ClusterEndpoint, acceptor: Acceptor): Unit = {
    logger.debug(s"Wait for authorizer to complete start up on listener ${clusterEndpoint.clusterName}")
    logger.debug(s"Start processors on listener ${clusterEndpoint.clusterName}")
    acceptor.startProcessors()
    logger.debug(s"Start acceptor thread on listener ${clusterEndpoint.clusterName}")
    if (!acceptor.isStarted()) {
      KafkaThread.nonDaemon(
        s"kafka-socket-acceptor-${clusterEndpoint.clusterName}-${clusterEndpoint.securityProtocol}",
        acceptor
      ).start()
      acceptor.awaitStartup()
    }
    logger.info(s"Started acceptor and processor(s) for endpoint : ${clusterEndpoint.clusterName}")
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown(): Unit = {
    logger.info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      requestChannel.shutdown()
    }
    logger.info("Shutdown completed")
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = {
    logger.info("Stopping socket server request processors")
    this.synchronized {
      acceptors.asScala.values.foreach(_.initiateShutdown())
      acceptors.asScala.values.foreach(_.awaitShutdown())
      requestChannel.clear()
      stoppedProcessingRequests = true
    }
    logger.info("Stopped socket server request processors")
  }

}
