package net.uweeisele.kafka.proxy.forward

import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}

import java.net.InetSocketAddress
import java.util.Optional

class ResponseContext(val remoteSocketAddress: InetSocketAddress,
                      val localSocketAddress: InetSocketAddress,
                      val principal: KafkaPrincipal,
                      val principalSerde: Optional[KafkaPrincipalSerde],
                      val listenerName: ListenerName,
                      val securityProtocol: SecurityProtocol,
                      var clientInformation: ClientInformation) {

    override def toString: String = s"ResponseContext(" +
      s"remoteSocketAddress=$remoteSocketAddress, " +
      s"localSocketAddress=$localSocketAddress, " +
      s"principal=$principal, " +
      s"listenerName=${listenerName.value}, " +
      s"securityProtocol=$securityProtocol, " +
      s"clientInformation=$clientInformation)"
}
