package net.uweeisele.kafka.proxy.cluster

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol

case class ClusterEndpoint(clusterName: ListenerName, securityProtocol: SecurityProtocol, brokerEndpoints: Seq[BrokerEndpoint]) {

}