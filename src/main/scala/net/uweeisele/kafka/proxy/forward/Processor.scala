package net.uweeisele.kafka.proxy.forward

import net.uweeisele.kafka.proxy.network.RequestChannel._
import net.uweeisele.kafka.proxy.network.{AbstractServerThread, ChannelBuilderBuilder, RequestChannel, RequestContext}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, CorrelationIdMismatchException, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.SocketChannel
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

sealed trait Command {val connectionId: String}
case class SendRequest(override val connectionId: String, request: RequestChannel.Request) extends Command
case class Connect(override val connectionId: String, address: InetSocketAddress) extends Command
case class Disconnect(override val connectionId: String) extends Command

object LocalClientInformation extends ClientInformation("apache-kafka-java", AppInfoParser.getVersion);

class Processor(val id: Int,
                val forwardChannel: ForwardChannel,
                val listenerName: ListenerName,
                val securityProtocol: SecurityProtocol,
                config: AbstractConfig,
                time: Time,
                maxRequestSize: Int,
                connectionsMaxIdleMs: Long,
                failedAuthenticationDelayMs: Int,
                memoryPool: MemoryPool,
                logContext: LogContext,
                sendBufferSize: Int,
                receiveBufferSize: Int,
                requestTimeoutMs: Long) extends AbstractServerThread {

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }

  private val inflightRequests = new InFlightRequests()
  private val commandQueue = new ConcurrentLinkedQueue[Command]()
  private val connections = mutable.Map[String,InetSocketAddress]()

  private val selector = createSelector(
    ChannelBuilderBuilder.build(
      Mode.CLIENT,
      listenerName,
      securityProtocol,
      config,
      logContext))
  // Visible to override for testing
  protected def createSelector(channelBuilder: ChannelBuilder): Selector = {
    new Selector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      new Metrics(),
      time,
      "forwarder",
      Map[String, String]().asJava,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  override def run(): Unit = {
    startupComplete()
    try {
      while (isRunning) {
        try {
          processNewCommands()
          processUnsent()
          poll()
          processCompletedSends()
          processCompletedReceives()
          processTimedOutRequests()
          processDisconnected()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      logger.debug(s"Closing selector - processor $id")
      try closeAll() catch { case e: Throwable => logger.error(e.getMessage, e) }
      shutdownComplete()
    }
  }

  private def processException(errorMessage: String, throwable: Throwable): Unit = {
    throwable match {
      case e: ControlThrowable => throw e
      case e => logger.error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable): Unit = {
    if (openOrClosingChannel(channelId).isDefined) {
      logger.error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewCommands(): Unit = {
    var currentCommand: Command = null
    while ({currentCommand = dequeueCommand(); currentCommand != null}) {
      val connectionId = currentCommand.connectionId
      try currentCommand match {
        case SendRequest(connectionId, request) =>
          sendRequest(connectionId, request)
        case Connect(connectionId, address) =>
          connect(connectionId, address)
        case Disconnect(connectionId) =>
          disconnect(connectionId)
      } catch {
        case e: Throwable =>
          processChannelException(connectionId, s"Exception while processing command ${currentCommand.getClass.getSimpleName} for $connectionId", e)
      }
    }
  }

  private def sendRequest(connectionId: String, request: RequestChannel.Request, onCompleteCallback: Option[Send => Unit] = None): Unit = {
    connections.get(connectionId) match {
      case Some(_) =>
        inflightRequests.add(new InFlightRequest(
          request,
          connectionId,
          onCompleteCallback,
          time.nanoseconds(),
          requestTimeoutMs
        ))
      case None =>
        logger.warn(s"Attempting to send request via channel for which there is no connection, connection id $connectionId")
        forwardChannel.sendResponse(new CloseConnectionResponse(request))
    }
  }

  private def processUnsent(): Unit = {
    inflightRequests.connections.forEach { connectionId =>
      openChannel(connectionId) match {
        case Some(channel) =>
          val it = inflightRequests.unset(connectionId).iterator
          while (channel.isConnected && channel.ready && it.hasNext) {
            val inFlightRequest = it.next
            val body = inFlightRequest.request.body[AbstractRequest]
            selector.send(new NetworkSend(connectionId, body.toSend(inFlightRequest.header)))
            inFlightRequest.setSent()
          }
          if (it.hasNext && (!channel.isConnected || !channel.ready)) {
            logger.info(s"Could not send request via connection $connectionId, because not ready")
          }
        case None =>
          logger.warn(s"Attempting to send request via channel for which there is no open connection, connection id $connectionId")
          initiateConnect(connectionId)
      }
    }
  }

  private def connect(connectionId: String, address: InetSocketAddress): Unit = {
    disconnect(connectionId)
    connections += connectionId -> address
    initiateConnect(connectionId)
  }

  private def initiateConnect(connectionId: String): Unit = {
    connections.get(connectionId) match {
      case Some(address) =>
        if (!address.isUnresolved) {
          logger.info(s"Connecting to target $address for connection $connectionId")
          selector.connect(connectionId, address, sendBufferSize, receiveBufferSize)
        } else {
          connections += connectionId -> new InetSocketAddress(address.getHostName, address.getPort)
        }
      case None =>
        logger.warn(s"Attempting to send request via channel for which there is no connection, connection id $connectionId")
    }
  }

  private def disconnect(connectionId: String): Unit = connections.remove(connectionId).foreach(_ => close(connectionId))

  private def poll(): Unit = {
    val pollTimeout = if (commandQueue.isEmpty) 300 else 0
    try selector.poll(pollTimeout)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        logger.error(s"Processor $id poll failed", e)
    }
  }

  private def processCompletedSends(): Unit = {
    //selector.completedSends().forEach(send => logger.info(s"Completed send for connection ${send.destinationId()}"))
    selector.clearCompletedSends()
  }

  private def processCompletedReceives(): Unit = {
    selector.completedReceives.forEach { receive =>
      try openOrClosingChannel(receive.source) match {
        case Some(channel) =>
          val source = receive.source
          val req = inflightRequests.completeNext(source)

          val response = parseResponse(receive.payload, req.header)

          logger.whenDebugEnabled {
            logger.debug("Received {} response from connection {} for request with header {}: {}",
              req.header.apiKey, req.connectionId, req.header, response)
          }

          val responseContext = new ResponseContext(remoteAddressFromChannel(channel), localAddressFromChannel(channel),
            channel.principal, channel.principalSerde, listenerName, securityProtocol, LocalClientInformation)
          val channelResponse = new SendResponse(req.request, response, responseContext, req.onCompleteCallback)
          forwardChannel.sendResponse(channelResponse)

        case None =>
          // This should never happen since completed receives are processed immediately after `poll()`
          throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
    selector.clearCompletedReceives()
  }

  def parseResponse(responseBuffer: ByteBuffer, requestHeader: RequestHeader): AbstractResponse = try AbstractResponse.parseResponse(responseBuffer, requestHeader)
  catch {
    case e: BufferUnderflowException =>
      throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e)
    case e: CorrelationIdMismatchException =>
      if (SaslClientAuthenticator.isReserved(requestHeader.correlationId) && !SaslClientAuthenticator.isReserved(e.responseCorrelationId)) throw new SchemaException("The response is unrelated to Sasl request since its correlation id is " + e.responseCorrelationId + " and the reserved range for Sasl request is [ " + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + "," + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]")
      else throw e
  }

  private def remoteAddressFromChannel(channel: KafkaChannel) = addressFromChannel(channel, socketChannel => socketChannel.getRemoteAddress)

  private def localAddressFromChannel(channel: KafkaChannel) = addressFromChannel(channel, socketChannel => socketChannel.getLocalAddress)

  private def addressFromChannel(channel: KafkaChannel, supplier: SocketChannel => SocketAddress): InetSocketAddress = channel.selectionKey().channel() match {
    case socketChannel: SocketChannel => supplier(socketChannel) match {
      case inetSocketAddress: InetSocketAddress => inetSocketAddress
      //case null => TODO!!!
      case address =>
        logger.warn(s"############# ${channel.id} no defined socket! connected: ${channel.isConnected} muted: ${channel.isMuted}")
        InetSocketAddress.createUnresolved("localhost", 1234) //throw new KafkaException(s"${channel.id} is not a InetSocketAddress, but ${address}! Should never be thrown!")
    }
    case _ => throw new KafkaException(s"${channel.id} is not a SocketChannel! Should never be thrown!")
  }

  private def processDisconnected(): Unit = {
    selector.disconnected.keySet.forEach { connectionId =>
      logger.info(s"Disconnected from $connectionId")
      inflightRequests.clearAll(connectionId)
        .foreach(inflightRequest => forwardChannel.sendResponse(new CloseConnectionResponse(inflightRequest.request)))
    }
  }

  /**
   * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
   * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
   *
   * @param responses The list of responses to update
   * @param now       The current time
   */
  private def processTimedOutRequests(): Unit = {
    val connectionIds = inflightRequests.connectionsWithTimedOutRequests(time.nanoseconds())
    for (connectionId <- connectionIds) { // close connection to the node
      close(connectionId)
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      logger.debug(s"Closing selector connection $connectionId")
      selector.close(connectionId)
      inflightRequests.clearAll(connectionId)
        .foreach(inflightRequest => forwardChannel.sendResponse(new CloseConnectionResponse(inflightRequest.request)))
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll(): Unit = {
    selector.channels.forEach { channel =>
      close(channel.id)
    }
    selector.close()
  }

  def enqueueCommand(command: Command): Unit = {
    commandQueue.add(command)
    wakeup()
  }

  private def dequeueCommand() = commandQueue.poll()

  def commandQueueSize = commandQueue.size

  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private def openOrClosingChannel(connectionId: String) =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  private def connectedChannel(connectionId: String) =
    Option(selector.channel(connectionId)).filter(c => c.isConnected && c.ready())

  private def openChannel(connectionId: String) =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  override def wakeup() = selector.wakeup()

}
