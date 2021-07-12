package net.uweeisele.kafka.proxy.filter.advertisedlistener

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.filter.ResponseFilter
import net.uweeisele.kafka.proxy.forward.RouteTable
import net.uweeisele.kafka.proxy.network.RequestChannel
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{FindCoordinatorResponse, MetadataResponse}

class AdvertisedListenerRewriteFilter(routeTable: RouteTable,
                                      advertisedListenerTable: AdvertisedListenerTable) extends ResponseFilter with LazyLogging {

  override def handle(response: RequestChannel.SendResponse): Unit = {
    response.response match {
      case metadataResponse: MetadataResponse =>
        handle(response.request.context.listenerNameRef, response.forwardContext.listenerNameRef, metadataResponse)
      case findCoordinatorResponse: FindCoordinatorResponse =>
        handle(response.request.context.listenerNameRef, response.forwardContext.listenerNameRef, findCoordinatorResponse)
      case _ =>
    }
  }

  private def handle(listenerName: ListenerName, targetListenerName: ListenerName, metadataResponse: MetadataResponse): Unit = {
    metadataResponse.data().brokers().forEach { broker =>
      routeTable
        .listenerEndpointByTarget(listenerName, targetListenerName, broker.host(), broker.port())
        .map(e => advertisedListenerTable.advertisedListenerRouteEndpointByListener(e)) match {
        case Some(endpoint) =>
          broker.setHost(endpoint.host)
          broker.setPort(endpoint.port)
        case None => logger.warn(s"Broker ${broker.host()}:${broker.port()} is unknown for target ${targetListenerName.value}.")
      }
    }
  }

  private def handle(listenerName: ListenerName, targetListenerName: ListenerName, findCoordinatorResponse: FindCoordinatorResponse): Unit = {
    if (findCoordinatorResponse.data().nodeId() >= 0) {
      routeTable
        .listenerEndpointByTarget(listenerName, targetListenerName, findCoordinatorResponse.data().host(), findCoordinatorResponse.data().port())
        .map(e => advertisedListenerTable.advertisedListenerRouteEndpointByListener(e)) match {
        case Some(endpoint) =>
          findCoordinatorResponse.data().setHost(endpoint.host)
          findCoordinatorResponse.data().setPort(endpoint.port)
        case None => logger.warn(s"Broker ${findCoordinatorResponse.data().host()}:${findCoordinatorResponse.data().port()} is unknown!")
      }
    }
  }

}
