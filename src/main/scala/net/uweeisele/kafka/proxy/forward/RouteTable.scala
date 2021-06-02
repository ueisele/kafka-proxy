package net.uweeisele.kafka.proxy.forward

import net.uweeisele.kafka.proxy.config.Endpoint
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.ListenerName

import java.net.InetSocketAddress
import scala.collection.Seq
import scala.collection.mutable.ListBuffer

case class RouteEndpoint(listenerName: ListenerName, host: String, port: Int)

class RouteTable(groupsListenerToTarget: Map[ListenerName, ListenerName],
                 listeners: Seq[Endpoint],
                 targets: Seq[Endpoint]) {

  private val endpointsListenerToTarget: Map[RouteEndpoint, RouteEndpoint] =
    groupsListenerToTarget.flatMap(e => createRouteEndpointsListenerToTargetForEndpoint(findEndpoint(listeners, e._1), findEndpoint(targets, e._2)))

  private val groupsTargetToListener: Map[ListenerName, Seq[ListenerName]] = {
    val map = new java.util.HashMap[ListenerName, ListBuffer[ListenerName]]()
    for ((listener,target) <- groupsListenerToTarget) {
      map.computeIfAbsent(target, k => ListBuffer[ListenerName]()) += listener
    }
    import scala.jdk.CollectionConverters._
    map.asScala.toMap
  }

  private val endpointsTargetToListener: Map[RouteEndpoint, Map[ListenerName,RouteEndpoint]] =
    groupsTargetToListener.flatMap(e => createRouteEndpointsTargetToListenerForEndpoint(findEndpoint(targets, e._1), e._2.map(v => findEndpoint(listeners, v))))

  def targetGroupByListener(listener: ListenerName): Option[ListenerName] = groupsListenerToTarget.get(listener)

  def targetEndpointByConnection(listenerName: ListenerName, address: InetSocketAddress): Option[RouteEndpoint] =
    targetEndpointByListener(listenerName, "0.0.0.0", address.getPort) orElse
      targetEndpointByListener(listenerName, address.getAddress.getHostAddress, address.getPort) orElse
      targetEndpointByListener(listenerName, address.getAddress.getHostName, address.getPort)

  def targetEndpointByListener(listenerName: ListenerName, host: String, port: Int): Option[RouteEndpoint] =
    endpointsListenerToTarget.get(RouteEndpoint(listenerName, host, port))

  def listenerEndpointByTarget(listenerName: ListenerName, targetListenerName: ListenerName, targetHost: String, targetPort: Int): Option[RouteEndpoint] =
    endpointsTargetToListener.get(RouteEndpoint(targetListenerName, targetHost, targetPort)) match {
      case Some(matchingListeners) => matchingListeners.get(listenerName)
      case None => None
    }

  private def findEndpoint(endpoints: Seq[Endpoint], listenerName: ListenerName): Endpoint =
    endpoints.find( e => e.listenerName.equals(listenerName)) match {
      case Some(value) => value
      case None => throw new ConfigException(s"No listener with name $listenerName found!")
    }

  private def createRouteEndpointsListenerToTargetForEndpoint(listener: Endpoint, target: Endpoint): Map[RouteEndpoint, RouteEndpoint] = {
    if (listener.bindings.size != target.bindings.size) {
      throw new ConfigException("Listener must have equal number of bindings than target!")
    }
    val listeners = listener.bindings.map(b => RouteEndpoint(listener.listenerName, b.host, b.port))
    val targets = target.bindings.map(b => RouteEndpoint(target.listenerName, b.host, b.port))
    listeners.zip(targets).toMap
  }

  private def createRouteEndpointsTargetToListenerForEndpoint(targetEndpoints: Endpoint, listenerEndpoints: Seq[Endpoint]): Map[RouteEndpoint, Map[ListenerName,RouteEndpoint]] = {
    listenerEndpoints.foreach { e =>
      if (targetEndpoints.bindings.size != e.bindings.size) {
        throw new ConfigException(s"Listener $e must have equal number of bindings than target $targetEndpoints!")
      }
    }
    val routeEndpoints = {
      for (bindingIdx <- targetEndpoints.bindings.indices)
        yield {
          val targetBinding = targetEndpoints.bindings(bindingIdx)
          val targetRouteEndpoint = RouteEndpoint(targetEndpoints.listenerName, targetBinding.host, targetBinding.port)
          val listenerRouteEndpoints: Map[ListenerName, RouteEndpoint] = listenerEndpoints
            .map(e => (e.listenerName,e.bindings(bindingIdx)))
            .map(v => (v._1, RouteEndpoint(v._1, normalizeAddress(v._2.host), v._2.port)))
            .toMap
          (targetRouteEndpoint, listenerRouteEndpoints)
        }
    }.toMap
    routeEndpoints
  }

  private def normalizeAddress(address: String): String =
    if(Some(address).isEmpty || address.isBlank)
      "0.0.0.0"
    else
      address

}