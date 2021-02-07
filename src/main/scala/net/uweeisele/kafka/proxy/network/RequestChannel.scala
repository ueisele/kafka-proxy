package net.uweeisele.kafka.proxy.network

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.request.RequestContext
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.requests.{AbstractRequest, RequestAndSize, RequestHeader}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.Sanitizer

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._
import scala.annotation.nowarn
import scala.reflect.ClassTag

object RequestChannel extends LazyLogging {

  sealed trait BaseRequest
  case object ShutdownRequest extends BaseRequest

  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress, localAddress: InetAddress) {
    val sanitizedUser: String = Sanitizer.sanitize(principal.getName)
  }

  class Request(val processor: Int,
                val context: RequestContext,
                val startTimeNanos: Long,
                memoryPool: MemoryPool,
                @volatile private var buffer: ByteBuffer) extends BaseRequest {

    val session: Session = Session(context.principal, context.clientAddress, context.localAddress)
    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    def header: RequestHeader = context.header
    def sizeOfBodyInBytes: Int = bodyAndSize.size

    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], @nowarn("cat=unused") nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    def releaseBuffer(): Unit = {
      if (buffer != null) {
        memoryPool.release(buffer)
        buffer = null
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer)"

  }

  abstract class Response(val request: Request) {

    def processor: Int = request.processor

    def onComplete: Option[Send => Unit] = None

    def toString: String
  }

  /** responseAsString should only be defined if request logging is enabled */
  class SendResponse(request: Request,
                     val responseSend: Send,
                     val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend)"
  }

  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }

  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }

  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }

  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}

class RequestChannel(val queueSize: Int) extends LazyLogging {
  import RequestChannel._
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
  private val processors = new ConcurrentHashMap[Int, Processor]()

  def addProcessor(processor: Processor): Unit = {
    if (processors.putIfAbsent(processor.id, processor) != null)
      logger.warn(s"Unexpected processor with processorId ${processor.id}")
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  /** Send a response back to the socket server to be sent over the network */
  def sendResponse(response: RequestChannel.Response): Unit = {
    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /** Get the next request or block until specified time has elapsed */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest =
    requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** Get the next request or block until there is one */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def clear(): Unit = {
    requestQueue.clear()
  }

  def shutdown(): Unit = {
    clear()
  }

  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}
