package net.uweeisele.kafka.proxy.response

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.forward.ForwardChannel
import net.uweeisele.kafka.proxy.network.RequestChannel
import org.apache.kafka.common.utils.KafkaThread

class ResponseHandlerPool(requestChannel: RequestChannel,
                          val forwardChannel: ForwardChannel,
                          apis: ApiResponseHandler,
                          numThreads: Int) extends LazyLogging {

  val runnables = List.tabulate(numThreads)(id => new ResponseHandler(id, requestChannel, forwardChannel, apis))

  def start(): Unit = synchronized {
    logger.info("starting")
    for (handler <- runnables)
      start(handler)
    for (handler <- runnables)
      handler.awaitStartup()
  }

  private def start(handler: ResponseHandler): Unit = {
    if (!handler.isStarted()) {
      KafkaThread.nonDaemon(
        s"kafka-response-handler-${handler.id}",
        handler
      ).start()
    }
  }

  def shutdown(): Unit = synchronized {
    logger.info("shutting down")
    for (handler <- runnables)
      forwardChannel.sendShutdownResponse()
    for (handler <- runnables)
      handler.awaitShutdown()
    logger.info("shut down completely")
  }

}
