package net.uweeisele.kafka.proxy.network

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.network._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.LogContext

import java.util

object ChannelBuilderBuilder {

  def build(mode: Mode,
            listenerName: ListenerName,
            securityProtocol: SecurityProtocol,
            config: AbstractConfig,
            logContext: LogContext): ChannelBuilder = {
    val configs = channelBuilderConfigs(config, listenerName)
    var channelBuilder: ChannelBuilder = null
    securityProtocol match {
      case SecurityProtocol.SSL =>
        requireNonNullMode(mode, securityProtocol)
        channelBuilder = new SslChannelBuilder(mode, listenerName, false, logContext)
      case SecurityProtocol.PLAINTEXT =>
        channelBuilder = new PlaintextChannelBuilder(listenerName)
      case _ =>
        throw new IllegalArgumentException("Unexpected securityProtocol " + securityProtocol)
    }
    channelBuilder.configure(configs)
    channelBuilder
  }

  /**
   * @return a mutable RecordingMap. The elements got from RecordingMap are marked as "used".
   */
  @SuppressWarnings(Array("unchecked"))
  private def channelBuilderConfigs(config: AbstractConfig, listenerName: ListenerName): util.Map[String, AnyRef] = {
    val parsedConfigs =
      if (listenerName == null) config.values.asInstanceOf[util.Map[String, AnyRef]]
      else config.valuesWithPrefixOverride(listenerName.configPrefix)

    config.originals().entrySet().stream()
      .filter(e => !parsedConfigs.containsKey(e.getKey())) // exclude already parsed configs
      // exclude already parsed listener prefix configs
      .filter(e => !(listenerName != null && e.getKey().startsWith(listenerName.configPrefix()) &&
        parsedConfigs.containsKey(e.getKey().substring(listenerName.configPrefix().length()))))
      // exclude keys like `{mechanism}.some.prop` if "listener.name." prefix is present and key `some.prop` exists in parsed configs.
      .filter(e => !(listenerName != null && parsedConfigs.containsKey(e.getKey().substring(e.getKey().indexOf('.') + 1))))
      .forEach(e => parsedConfigs.put(e.getKey(), e.getValue()));
    return parsedConfigs;
  }

  private def requireNonNullMode(mode: Mode, securityProtocol: SecurityProtocol): Unit = {
    if (mode == null) throw new IllegalArgumentException("`mode` must be non-null if `securityProtocol` is `" + securityProtocol + "`")
  }
}
