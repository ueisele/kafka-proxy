package net.uweeisele.kafka.proxy.request

import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.requests.{RequestHeader, RequestContext => JRequestContext}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}

import java.net.InetAddress
class RequestContext(header: RequestHeader,
                     connectionId: String,
                     clientAddress: InetAddress,
                     val localAddress: InetAddress,
                     principal: KafkaPrincipal,
                     listenerName: ListenerName,
                     securityProtocol: SecurityProtocol,
                     clientInformation: ClientInformation)
  extends JRequestContext(
    header,
    connectionId,
    clientAddress,
    principal,
    listenerName,
    securityProtocol,
    clientInformation) {

}
