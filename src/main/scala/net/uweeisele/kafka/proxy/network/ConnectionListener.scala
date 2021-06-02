package net.uweeisele.kafka.proxy.network

import org.apache.kafka.common.network.ListenerName

import java.net.InetSocketAddress

trait ConnectionListener {

  def onConnectionEstablished(listener: ListenerName, connectionId: String, address: InetSocketAddress): Unit

  def onConnectionClosed(listener: ListenerName, connectionId: String): Unit

}
