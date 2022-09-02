package net.uweeisele.kafka.proxy.filter.apiversion

import com.typesafe.scalalogging.LazyLogging
import net.uweeisele.kafka.proxy.network.RequestChannel
import net.uweeisele.kafka.proxy.response.ApiResponseHandler
import org.apache.kafka.common.message.ApiMessageType
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion
import org.apache.kafka.common.requests.ApiVersionsResponse

class ApiVersionFilter extends ApiResponseHandler with LazyLogging {

  private val supportedApiMessageTypes: Map[Short, ApiMessageType] = ApiMessageType.values()
    .map(apiMessageType => (apiMessageType.apiKey(), apiMessageType)).toMap

  override def handle(response: RequestChannel.SendResponse): Unit = {
    response.response match {
      case apiVersionResponse: ApiVersionsResponse =>
        handle(apiVersionResponse)
      case _ =>
    }
  }

  private def handle(apiVersionResponse: ApiVersionsResponse ): Unit = {
    val it = apiVersionResponse.data().apiKeys().iterator()
    while (it.hasNext) {
      val apiVersion = it.next();
      supportedApiMessageTypes.get(apiVersion.apiKey()) match {
        case Some(supportedApiMessageType) =>
          if ((apiVersion.minVersion() > supportedApiMessageType.highestSupportedVersion()) ||
              (apiVersion.maxVersion() < supportedApiMessageType.lowestSupportedVersion())) {
            it.remove()
          } else {
            if (apiVersion.minVersion() < supportedApiMessageType.lowestSupportedVersion()) {
              apiVersion.setMinVersion(supportedApiMessageType.lowestSupportedVersion())
            }
            if (apiVersion.maxVersion() > supportedApiMessageType.highestSupportedVersion()) {
              apiVersion.setMaxVersion(supportedApiMessageType.highestSupportedVersion())
            }
          }
        case None => it.remove()
      }
    }
  }
}
