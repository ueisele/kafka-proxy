package net.uweeisele.kafka.proxy.request

import net.uweeisele.kafka.proxy.network.{AbstractServerThread, RequestChannel}

trait ApiRequestHandler {
  def handle(request: RequestChannel.Request): Unit
}

class RequestHandler(val id: Int,
                     requestChannel: RequestChannel,
                     apis: ApiRequestHandler) extends AbstractServerThread {

  override def wakeup(): Unit = ???

  override def run(): Unit = {
    startupComplete()
    try {
      while (isRunning) {
        requestChannel.receiveRequest(500) match {
          case RequestChannel.ShutdownRequest =>
            logger.info(s"Request handler $id received shut down command")
            initiateShutdown()

          case request: RequestChannel.Request =>
            try {
              apis.handle(request)
            } catch {
              case e: Throwable => logger.error(s"Exception when handling request on request handler $id", e)
            } finally {
              request.releaseBuffer()
            }

          case null => //continue
        }
      }
    } finally {
      shutdownComplete()
    }
  }

}
