package net.uweeisele.kafka.proxy.response

import net.uweeisele.kafka.proxy.network.RequestChannel

trait ApiResponseHandler {
  def handle(response: RequestChannel.SendResponse): Unit
}

class ApiResponseHandlerChain(handlerChain: Seq[ApiResponseHandler]) extends ApiResponseHandler {
  override def handle(response: RequestChannel.SendResponse): Unit = handlerChain.foreach(h => h.handle(response))
}

object ApiResponseHandlerChain {
  def apply(handlerChain: Seq[ApiResponseHandler]): ApiResponseHandler = new ApiResponseHandlerChain(handlerChain)
}