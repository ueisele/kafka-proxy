package net.uweeisele.kafka.proxy.request

import org.apache.kafka.common.network.{ClientInformation, ListenerName, Send}
import org.apache.kafka.common.requests.{AbstractResponse, RequestAndSize, RequestHeader, RequestContext => JRequestContext}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.server.authorizer.AuthorizableRequestContext

import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.util.Optional
import scala.collection.mutable

class RequestContext(val header: RequestHeader,
                     val connectionId: String,
                     val clientSocketAddress: InetSocketAddress,
                     val localSocketAddress: InetSocketAddress,
                     override val principal: KafkaPrincipal,
                     val listenerNameRef: ListenerName,
                     override val securityProtocol: SecurityProtocol,
                     val clientInformation: ClientInformation,
                     principalSerde: Optional[KafkaPrincipalSerde],
                     val variables: mutable.Map[String, Any] = mutable.Map())
  extends AuthorizableRequestContext {

    private val internalContext = new JRequestContext(
        header,
        connectionId,
        clientSocketAddress.getAddress,
        principal,
        listenerNameRef,
        securityProtocol,
        clientInformation,
        true,
        principalSerde)

    def parseRequest(buffer: ByteBuffer): RequestAndSize = internalContext.parseRequest(buffer)

    def buildResponseSend(body: AbstractResponse): Send = internalContext.buildResponseSend(body)

    def apiVersion: Short = internalContext.apiVersion()

    override def listenerName(): String = listenerNameRef.value()

    override def clientAddress(): InetAddress = clientSocketAddress.getAddress

    override def requestType(): Int = header.apiKey().id

    override def requestVersion(): Int = header.apiVersion()

    override def clientId(): String = header.clientId()

    override def correlationId(): Int = header.correlationId()

    override def toString = s"RequestContext(" +
      s"header=$header, " +
      s"connectionId=$connectionId, " +
      s"clientSocketAddress=$clientSocketAddress, " +
      s"localSocketAddress=$localSocketAddress, " +
      s"principal=$principal, " +
      s"listenerName=$listenerName, " +
      s"securityProtocol=$securityProtocol, " +
      s"clientInformation=$clientInformation)"
}
