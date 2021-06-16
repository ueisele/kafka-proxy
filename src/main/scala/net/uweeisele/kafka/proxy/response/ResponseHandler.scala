package net.uweeisele.kafka.proxy.response

import net.uweeisele.kafka.proxy.forward.ForwardChannel
import net.uweeisele.kafka.proxy.network.RequestChannel.CloseConnectionResponse
import net.uweeisele.kafka.proxy.network.{AbstractServerThread, RequestChannel}

class ResponseHandler(val id: Int,
                      requestChannel: RequestChannel,
                      forwardChannel: ForwardChannel,
                      apis: ApiResponseHandler) extends AbstractServerThread {

  override def wakeup(): Unit = {}

  override def run(): Unit = {
    startupComplete()
    try {
      while (isRunning) {
        forwardChannel.receiveResponse(500) match {
          case RequestChannel.ShutdownResponse =>
            logger.info(s"Response handler $id received shut down command")
            initiateShutdown()

          case response: RequestChannel.SendResponse =>
            try {
              apis.handle(response)
              requestChannel.sendResponse(response)
            } catch {
              case e: Throwable =>
                logger.error(s"Exception when handling response on response handler $id", e)
                requestChannel.sendResponse(new CloseConnectionResponse(response.request))
            }

          case response: RequestChannel.Response =>
            requestChannel.sendResponse(response)

          case null => //continue
        }
      }
    } finally {
      shutdownComplete()
    }
  }

}
