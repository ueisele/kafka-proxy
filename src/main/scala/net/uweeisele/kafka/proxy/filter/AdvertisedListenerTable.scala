package net.uweeisele.kafka.proxy.filter

import net.uweeisele.kafka.proxy.config.{Binding, Endpoint}
import net.uweeisele.kafka.proxy.forward.RouteEndpoint
import org.apache.kafka.common.config.ConfigException

import java.net.{InetAddress, UnknownHostException}
import scala.collection.Seq

class AdvertisedListenerTable(val listeners: Seq[Endpoint],
                              val configuredAdvertisedListeners: Seq[Endpoint]) {

  val advertisedListeners: Seq[Endpoint] =
    listeners.map{ e =>
      configuredAdvertisedListeners.find(a => a.listenerName.equals(e.listenerName)) match {
        case Some(advertisedListener) =>
          if(advertisedListener.bindings.size != e.bindings.size) {
            throw new ConfigException(s"Listener $e must have equal number of bindings than advertised listeners $advertisedListener!")
          }
          advertisedListener
        case None =>
          new Endpoint(e.listenerName, e.securityProtocol, e.bindings.map(b => new Binding(resolveAdvertisedHost(b.host), b.port)))
      }
    }

  val advertisedListenerByListener: Map[Endpoint, Endpoint] = listeners.zip(advertisedListeners).toMap

  val advertisedListenerRouteEndpointByListener: Map[RouteEndpoint, RouteEndpoint] =
    advertisedListenerByListener.flatMap(e => createRouteEndpointsMapForEndpoint(e._1, e._2))


  private def resolveAdvertisedHost(host: String): String = {
    if(Some(host).isEmpty || host.isBlank || host.equals("0.0.0.0")) {
      InetAddress.getLocalHost.getHostName
    }
    requireValidAddress(host)
  }

  private def requireValidAddress(address: String): String = {
    try InetAddress.getByName(address)
    catch {
      case e: UnknownHostException => throw new ConfigException(s"$address is not a valid address!", e)
    }
    address
  }

  private def createRouteEndpointsMapForEndpoint(a: Endpoint, b: Endpoint): Map[RouteEndpoint, RouteEndpoint] = {
    val as = a.bindings.map(v => RouteEndpoint(a.listenerName, v.host, v.port))
    val bs = b.bindings.map(v => RouteEndpoint(b.listenerName, v.host, v.port))
    as.zip(bs).toMap
  }
}
