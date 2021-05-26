package net.uweeisele.kafka.proxy.request

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.network.RequestChannel
import org.apache.kafka.common.utils.KafkaThread

class RequestHandlerPool(val requestChannel: RequestChannel,
                         apis: ApiRequestHandler,
                         numThreads: Int) extends LazyLogging {

  val runnables = List.tabulate(numThreads)(id => new RequestHandler(id, requestChannel, apis))

  def start(): Unit = synchronized {
    logger.info("starting")
    for (handler <- runnables)
      start(handler)
    for (handler <- runnables)
      handler.awaitStartup()
  }

  private def start(handler: RequestHandler): Unit = {
    if (!handler.isStarted()) {
      KafkaThread.nonDaemon(
        s"kafka-request-handler-${handler.id}",
        handler
      ).start()
    }
  }

  def shutdown(): Unit = synchronized {
    logger.info("shutting down")
    for (handler <- runnables)
      requestChannel.sendShutdownRequest()
    for (handler <- runnables)
      handler.awaitShutdown()
    logger.info("shut down completely")
  }

}
