package net.uweeisele.kafka.proxy.network

import net.uweeisele.kafka.proxy.config.{Binding, Endpoint}
import net.uweeisele.kafka.proxy.network.Acceptor.USE_DEFAULT_BUFFER_SIZE
import org.apache.kafka.common.utils.KafkaThread

import java.net.{InetSocketAddress, SocketException}
import java.nio.channels.{SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.Seq
import scala.collection.mutable.{ArrayBuffer, Buffer, ListBuffer}
import scala.util.control.ControlThrowable

object Acceptor {
  val USE_DEFAULT_BUFFER_SIZE: Int = -1
}

class Acceptor(val endpoint: Endpoint,
               recvBufferSize: Int,
               sendBufferSize: Int) extends AbstractServerThread {

  private val nioSelector: Selector = Selector.open()
  private val serverChannels: Seq[ServerSocketChannel] = endpoint.bindings.map(openServerSocket)
  private val processors = new ArrayBuffer[Processor]()
  private val processorsStarted = new AtomicBoolean
  private val connectionListeners = ListBuffer[ConnectionListener]()

  override def wakeup(): Unit = nioSelector.wakeup()

  private def openServerSocket(endpoint: Binding): ServerSocketChannel = {
    val socketAddress =
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        new InetSocketAddress(endpoint.port)
      else
        new InetSocketAddress(endpoint.host, endpoint.port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket.setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.bind(socketAddress)
      logger.info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new ServerException(s"Socket server failed to bind to ${socketAddress.getHostString}:${endpoint.port}: ${e.getMessage}.", e)
    }
    serverChannel
  }

  def addConnectionListener(connectionListener: ConnectionListener): Unit = {
    connectionListeners += connectionListener
    processors.foreach(p => p.addConnectionListener(connectionListener))
  }

  private[network] def addProcessors(newProcessors: Buffer[Processor]): Unit = synchronized {
    newProcessors.foreach(p => connectionListeners.foreach(l => p.addConnectionListener(l)))
    processors ++= newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors)
  }

  private[network] def startProcessors(): Unit = synchronized {
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors)
    }
  }

  private def startProcessors(processors: Seq[Processor]): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(
        s"kafka-proxy-network-thread-${endpoint.listenerName}-${endpoint.securityProtocol}-${processor.id}",
        processor
      ).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)
    toRemove.foreach(_.initiateShutdown())
    toRemove.foreach(_.awaitShutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    synchronized {
      processors.foreach(_.initiateShutdown())
    }
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    synchronized {
      processors.foreach(_.awaitShutdown())
    }
  }

  override def run(): Unit = {
    serverChannels.foreach(serverChannel => serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT))
    startupComplete()
    try {
      var currentProcessorIndex = 0
      while (isRunning) {
        try {
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()

                if (key.isAcceptable) {
                  accept(key).foreach { socketChannel =>
                    // Assign the channel to the next processor (using round-robin) to which the
                    // channel can be added without blocking. If newConnections queue is full on
                    // all processors, block until the last one is able to accept a connection.
                    var retriesLeft = synchronized(processors.length)
                    var processor: Processor = null
                    while ({ {
                      retriesLeft -= 1
                      processor = synchronized {
                        // adjust the index (if necessary) and retrieve the processor atomically for
                        // correct behaviour in case the number of processors is reduced dynamically
                        currentProcessorIndex = currentProcessorIndex % processors.length
                        processors(currentProcessorIndex)
                      }
                      currentProcessorIndex += 1
                    } ; !assignNewConnection(socketChannel, processor, retriesLeft == 0)}) ()
                  }
                } else {
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")
                }
              } catch {
                case e: Throwable => logger.error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the server to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => logger.error("Error occurred", e)
        }
      }
    } finally {
      logger.debug("Closing server socket and selector.")
      for (serverChannel <- serverChannels) {
        try {serverChannel.close()} catch {case e: Throwable => logger.error(e.getMessage, e)}
      }
      try {nioSelector.close()} catch { case e: Throwable => logger.error(e.getMessage, e) }
      shutdownComplete()
    }
  }

  private def accept(key: SelectionKey): Option[SocketChannel] = {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    socketChannel.socket().setKeepAlive(true)
    if (sendBufferSize != USE_DEFAULT_BUFFER_SIZE)
      socketChannel.socket().setSendBufferSize(sendBufferSize)
    Some(socketChannel)
  }

  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {
    if (processor.accept(socketChannel, mayBlock)) {
      logger.debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

}
