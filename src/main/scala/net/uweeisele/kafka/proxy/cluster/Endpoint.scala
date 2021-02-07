package net.uweeisele.kafka.proxy.cluster

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, Endpoint => JEndpoint}

import scala.collection.{Map, Seq}

object Binding {

  private val bindingParseExp = """^\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)""".r

  def createBinding(bindingString: String): Binding = {
    bindingString match {
      case bindingParseExp("", port) =>
        Binding(null, port.toInt)
      case bindingParseExp(host, port) =>
        Binding(host, port.toInt)
      case _ => throw new KafkaException(s"Unable to parse $bindingString to a binding")
    }
  }
}

object Endpoint {

  private val uriParseExp = """^(.*)://(.*)""".r

  private[kafka] val DefaultSecurityProtocolMap: Map[ListenerName, SecurityProtocol] =
    SecurityProtocol.values.map(sp => ListenerName.forSecurityProtocol(sp) -> sp).toMap

  /**
   * Create Endpoint object from `connectionString` and optional `securityProtocolMap`. If the latter is not provided,
   * we fallback to the default behaviour where listener names are the same as security protocols.
   *
   * @param connectionString the format is listener_name://host:port,host:port,... or listener_name://[ipv6 host]:port,[ipv6 host]:port,...
   *                         for example: PLAINTEXT://myhost:9092, CLIENT://myhost:9092,myhost:9093 or REPLICATION://[::1]:9092,[::1]:9093
   *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
   *                         Negative ports are also accepted, since they are used in some unit tests
   */
  def createEndPoint(connectionString: String, securityProtocolMap: Option[Map[ListenerName, SecurityProtocol]] = None): Endpoint = {
    val protocolMap = securityProtocolMap.getOrElse(DefaultSecurityProtocolMap)

    def securityProtocol(listenerName: ListenerName): SecurityProtocol =
      protocolMap.getOrElse(listenerName,
        throw new IllegalArgumentException(s"No security protocol defined for listener ${listenerName.value}"))

    connectionString match {
      case uriParseExp(listenerNameString, bindingsString) =>
        val listenerName = ListenerName.normalised(listenerNameString)
        Endpoint(listenerName, securityProtocol(listenerName), parseCsvList(bindingsString).map(Binding.createBinding))
      case _ => throw new KafkaException(s"Unable to parse $connectionString to a broker endpoint")
    }
  }

  private def parseCsvList(csvList: String): Seq[String] = {
    if (csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
  }

}

case class Binding(host: String, port: Int) {

  override def toString: String = {
    if (host == null)
      ":"+port
    else {
      Utils.formatAddress(host, port)
    }
  }

}

case class Endpoint(listenerName: ListenerName, securityProtocol: SecurityProtocol, bindings: Seq[Binding]) {

  def connectionString: String = listenerName.value + "://" + bindings.mkString(",")

  def toJava: Seq[JEndpoint] = bindings.map(b => new JEndpoint(listenerName.value, securityProtocol, b.host, b.port))
}