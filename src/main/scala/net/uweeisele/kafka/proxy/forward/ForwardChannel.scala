package net.uweeisele.kafka.proxy.forward

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.network.RequestChannel.{Response, ShutdownResponse}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.Utils.{murmur2, toPositive}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, TimeUnit}
import scala.collection.mutable.ListBuffer

class ForwardChannel(val queueSize: Int) extends LazyLogging {

  private val processorGroups = new ConcurrentHashMap[ListenerName, ListBuffer[Int]]()
  private val processors = new ConcurrentHashMap[Int, Processor]()
  private val responseQueue = new ArrayBlockingQueue[Response](queueSize)

  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) == null) {
      processorGroups.computeIfAbsent(processor.listenerName, listenerName => ListBuffer()) += processor.id
    } else {
      logger.warn(s"Unexpected processor with processorId ${processor.id}")
    }
  }

  def removeProcessor(processorId: Int): Unit = {
    Option(processors.remove(processorId)) match {
      case Some(processor) =>
        processorGroups.computeIfPresent(processor.listenerName, (listenerName, group) => group diff List(processorId))
        processorGroups.remove(processor.listenerName, Nil)
      case None =>
    }
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request, target: ListenerName, connectionId: String): Unit = {
    findProcessor(target, connectionId) match {
      case Some(processor) => processor.enqueueCommand(SendRequest(connectionId, request))
      case None =>
    }
  }

  def connect(target: ListenerName, connectionId: String, address: InetSocketAddress): Unit = {
    findProcessor(target, connectionId) match {
      case Some(processor) => processor.enqueueCommand(Connect(connectionId, address))
      case None =>
    }
  }

  def disconnect(target: ListenerName, connectionId: String): Unit = {
    findProcessor(target, connectionId) match {
      case Some(processor) => processor.enqueueCommand(Disconnect(connectionId))
      case None =>
    }
  }

  private def findProcessor(target: ListenerName, connectionId: String): Option[Processor] = {
    def group = processorGroups.getOrDefault(target, ListBuffer())
    if (group.nonEmpty) {
      def idx = toPositive(murmur2(connectionId.getBytes(UTF_8))) % group.size
      Some(processors.get(group(idx)))
    } else {
      None
    }
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveResponse(timeout: Long): RequestChannel.Response =
    responseQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveResponse(): RequestChannel.Response =
    responseQueue.take()

  def clear(): Unit = {
    responseQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
  }

  def sendShutdownResponse(): Unit = responseQueue.put(ShutdownResponse)

}
