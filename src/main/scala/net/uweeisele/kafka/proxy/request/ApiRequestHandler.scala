package net.uweeisele.kafka.proxy.request

import net.uweeisele.kafka.proxy.network.RequestChannel

import scala.language.implicitConversions

trait ApiRequestHandler {
  def handle(request: RequestChannel.Request): Unit
}

class ApiRequestHandlerChain(handlerChain: Seq[ApiRequestHandler]) extends ApiRequestHandler {
  override def handle(request: RequestChannel.Request): Unit = handlerChain.foreach(h => h.handle(request))
}

object ApiRequestHandlerChain {
  def apply(handlerChain: Seq[ApiRequestHandler]): ApiRequestHandler = new ApiRequestHandlerChain(handlerChain)
}