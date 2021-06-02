package net.uweeisele.kafka.proxy.request

import net.uweeisele.kafka.proxy.network.RequestChannel

trait ApiRequestHandler {
  def handle(request: RequestChannel.Request): Unit
}

class ApiRequestHandlerChain(handlerChain: Seq[ApiRequestHandler]) extends ApiRequestHandler {
  override def handle(request: RequestChannel.Request): Unit = handlerChain.foreach(h => h.handle(request))
}
