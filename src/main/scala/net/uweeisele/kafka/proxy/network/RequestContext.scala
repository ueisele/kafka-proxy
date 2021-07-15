package net.uweeisele.kafka.proxy.network

import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{AbstractResponse, RequestAndSize, RequestHeader, RequestContext => JRequestContext}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.Optional
import scala.collection.mutable

class RequestContext(val header: RequestHeader,
                     val connectionId: String,
                     val clientSocketAddress: InetSocketAddress,
                     val localSocketAddress: InetSocketAddress,
                     val principal: KafkaPrincipal,
                     val principalSerde: Optional[KafkaPrincipalSerde],
                     val listenerName: ListenerName,
                     val securityProtocol: SecurityProtocol,
                     private[network] var _clientInformation: ClientInformation = ClientInformation.EMPTY,
                     val variables: mutable.Map[String, Any] = mutable.Map()) {

    private val internalContext = new JRequestContext(
        header,
        connectionId,
        clientSocketAddress.getAddress,
        principal,
        listenerName,
        securityProtocol,
        null,
        false,
        null)

    private[network] def parseRequest(buffer: ByteBuffer): RequestAndSize = internalContext.parseRequest(buffer)

    private[network] def buildResponseSend(body: AbstractResponse): Send = internalContext.buildResponseSend(body)

    def apiVersion: Short = internalContext.apiVersion

    def apiKey: ApiKeys = header.apiKey

    def clientId: String = header.clientId

    def clientInformation: ClientInformation = _clientInformation

    private[network] def clientInformation_= (newVal:ClientInformation): Unit = _clientInformation = newVal

    def correlationId: Int = header.correlationId

    override def toString: String = s"RequestContext(" +
      s"header=$header, " +
      s"connectionId=$connectionId, " +
      s"clientSocketAddress=$clientSocketAddress, " +
      s"localSocketAddress=$localSocketAddress, " +
      s"principal=$principal, " +
      s"listenerName=${listenerName.value}, " +
      s"securityProtocol=$securityProtocol, " +
      s"clientInformation=$clientInformation)"

}
