package net.uweeisele.kafka.proxy.config

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.ListenerName

import scala.collection.Seq

object Route {

  def createRouteMap(routesString: String): Map[ListenerName, ListenerName] =
    parseCsvList(routesString).map(routeString => parseRoute(routeString)).toMap

  private def parseCsvList(csvList: String): Seq[String] = {
    if (csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
  }

  private def parseRoute(route: String): (ListenerName, ListenerName) = {
    val names = route.split("\\s*->\\s*", 2)
    if (names.size != 2) {
      throw new ConfigException(s"$route is not a vaild route definition!")
    }
    (new ListenerName(names(0)), new ListenerName(names(1)))
  }
}