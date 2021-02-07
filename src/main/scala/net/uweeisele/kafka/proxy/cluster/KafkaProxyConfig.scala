package net.uweeisele.kafka.proxy.cluster

import java.util
import java.util.{Collections, Locale, Properties}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.SecurityConfig
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidList}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, SaslConfigs, SslClientAuth, SslConfigs, TopicConfig}
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.{LegacyRecord, Records, TimestampType}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.authorizer.Authorizer

import java.security.CryptoPrimitive
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, mutable}

object Defaults {

  /** ********* General Configuration ***********/
  val NumNetworkThreads = 3
  val QueuedMaxRequests = 500
  val QueuedMaxRequestBytes = -1

  val ListenerSecurityProtocolMap: String = EndPoint.DefaultSecurityProtocolMap.map { case (listenerName, securityProtocol) =>
    s"${listenerName.value}:${securityProtocol.name}"
  }.mkString(",")

  val SocketSendBufferBytes: Int = 100 * 1024
  val SocketReceiveBufferBytes: Int = 100 * 1024
  val SocketRequestMaxBytes: Int = 100 * 1024 * 1024
  val ConnectionsMaxIdleMs = 10 * 60 * 1000L
  val FailedAuthenticationDelayMs = 100

  /** ********* SSL configuration ***********/
  val SslProtocol = SslConfigs.DEFAULT_SSL_PROTOCOL
  val SslEnabledProtocols = SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS
  val SslKeystoreType = SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE
  val SslTruststoreType = SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE
  val SslKeyManagerAlgorithm = SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM
  val SslTrustManagerAlgorithm = SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM
  val SslEndpointIdentificationAlgorithm = SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM
  val SslClientAuthentication = SslClientAuth.NONE.name().toLowerCase(Locale.ROOT)
  val SslClientAuthenticationValidValues = SslClientAuth.VALUES.asScala.map(v => v.toString().toLowerCase(Locale.ROOT)).asJava.toArray(new Array[String](0))
  val SslPrincipalMappingRules = BrokerSecurityConfigs.DEFAULT_SSL_PRINCIPAL_MAPPING_RULES

  /** ********* General Security configuration ***********/
  val ConnectionsMaxReauthMsDefault = 0L

  /** ********* Sasl configuration ***********/
  val SaslEnabledMechanisms = BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS
  val SaslKerberosKinitCmd = SaslConfigs.DEFAULT_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactor = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitter = SaslConfigs.DEFAULT_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeRelogin = SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRules = BrokerSecurityConfigs.DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES
  val SaslLoginRefreshWindowFactor = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitter = SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSeconds = SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS

  /** ********* Delegation Token configuration ***********/
  val DelegationTokenMaxLifeTimeMsDefault = 7 * 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryTimeMsDefault = 24 * 60 * 60 * 1000L
  val DelegationTokenExpiryCheckIntervalMsDefault = 1 * 60 * 60 * 1000L

}

object KafkaProxyConfig {

  private val LogConfigPrefix = "log."

  def main(args: Array[String]): Unit = {
    System.out.println(configDef.toHtml(4, (config: String) => "brokerconfigs_" + config))
  }

  /** ********* General Configuration ***********/
  val NumNetworkThreadsProp = "num.network.threads"
  val QueuedMaxRequestsProp = "queued.max.requests"
  val QueuedMaxBytesProp = "queued.max.request.bytes"
  /** ********* Socket Server Configuration ***********/
  val ListenersProp = "listeners"
  val ListenerSecurityProtocolMapProp = "listener.security.protocol.map"
  val SocketSendBufferBytesProp = "socket.send.buffer.bytes"
  val SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes"
  val SocketRequestMaxBytesProp = "socket.request.max.bytes"
  val ConnectionsMaxIdleMsProp = "connections.max.idle.ms"
  val FailedAuthenticationDelayMsProp = "connection.failed.authentication.delay.ms"

  /** ******** Common Security Configuration *************/
  val PrincipalBuilderClassProp = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG
  val ConnectionsMaxReauthMsProp = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS
  val securityProviderClassProp = SecurityConfig.SECURITY_PROVIDERS_CONFIG

  /** ********* SSL Configuration ****************/
  val SslProtocolProp = SslConfigs.SSL_PROTOCOL_CONFIG
  val SslProviderProp = SslConfigs.SSL_PROVIDER_CONFIG
  val SslCipherSuitesProp = SslConfigs.SSL_CIPHER_SUITES_CONFIG
  val SslEnabledProtocolsProp = SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG
  val SslKeystoreTypeProp = SslConfigs.SSL_KEYSTORE_TYPE_CONFIG
  val SslKeystoreLocationProp = SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG
  val SslKeystorePasswordProp = SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
  val SslKeyPasswordProp = SslConfigs.SSL_KEY_PASSWORD_CONFIG
  val SslKeystoreKeyProp = SslConfigs.SSL_KEYSTORE_KEY_CONFIG
  val SslKeystoreCertificateChainProp = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG
  val SslTruststoreTypeProp = SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG
  val SslTruststoreLocationProp = SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
  val SslTruststorePasswordProp = SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG
  val SslTruststoreCertificatesProp = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG
  val SslKeyManagerAlgorithmProp = SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG
  val SslTrustManagerAlgorithmProp = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG
  val SslEndpointIdentificationAlgorithmProp = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
  val SslSecureRandomImplementationProp = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG
  val SslClientAuthProp = BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG
  val SslPrincipalMappingRulesProp = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG
  var SslEngineFactoryClassProp = SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG

  /** ********* SASL Configuration ****************/
  val SaslJaasConfigProp = SaslConfigs.SASL_JAAS_CONFIG
  val SaslEnabledMechanismsProp = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG
  val SaslServerCallbackHandlerClassProp = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS
  val SaslClientCallbackHandlerClassProp = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS
  val SaslLoginClassProp = SaslConfigs.SASL_LOGIN_CLASS
  val SaslLoginCallbackHandlerClassProp = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS
  val SaslKerberosServiceNameProp = SaslConfigs.SASL_KERBEROS_SERVICE_NAME
  val SaslKerberosKinitCmdProp = SaslConfigs.SASL_KERBEROS_KINIT_CMD
  val SaslKerberosTicketRenewWindowFactorProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR
  val SaslKerberosTicketRenewJitterProp = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER
  val SaslKerberosMinTimeBeforeReloginProp = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN
  val SaslKerberosPrincipalToLocalRulesProp = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG
  val SaslLoginRefreshWindowFactorProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR
  val SaslLoginRefreshWindowJitterProp = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER
  val SaslLoginRefreshMinPeriodSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS
  val SaslLoginRefreshBufferSecondsProp = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS

  /* Documentation */
  /** ********* General Configuration ***********/
  val NumNetworkThreadsDoc = "The number of threads that the server uses for receiving requests from the network and sending responses to the network"
  val QueuedMaxRequestsDoc = "The number of queued requests allowed for data-plane, before blocking the network threads"
  val QueuedMaxRequestBytesDoc = "The number of queued bytes allowed before no more requests are read"
  /** ********* Socket Server Configuration ***********/
  val ListenersDoc = "Listener List - Comma-separated list of URIs we will listen on and the listener names." +
    s" If the listener name is not a security protocol, <code>$ListenerSecurityProtocolMapProp</code> must also be set.\n" +
    " Listener names and port numbers must be unique.\n" +
    " Specify hostname as 0.0.0.0 to bind to all interfaces.\n" +
    " Leave hostname empty to bind to default interface.\n" +
    " Examples of legal listener lists:\n" +
    " PLAINTEXT://myhost:9092,SSL://:9091\n" +
    " CLIENT://0.0.0.0:9092,REPLICATION://localhost:9093\n"
  val ListenerSecurityProtocolMapDoc = "Map between listener names and security protocols. This must be defined for " +
    "the same security protocol to be usable in more than one port or IP. For example, internal and " +
    "external traffic can be separated even if SSL is required for both. Concretely, the user could define listeners " +
    "with names INTERNAL and EXTERNAL and this property as: `INTERNAL:SSL,EXTERNAL:SSL`. As shown, key and value are " +
    "separated by a colon and map entries are separated by commas. Each listener name should only appear once in the map. " +
    "Different security (SSL and SASL) settings can be configured for each listener by adding a normalised " +
    "prefix (the listener name is lowercased) to the config name. For example, to set a different keystore for the " +
    "INTERNAL listener, a config with name <code>listener.name.internal.ssl.keystore.location</code> would be set. " +
    "If the config for the listener name is not set, the config will fallback to the generic config (i.e. <code>ssl.keystore.location</code>). "

  val SocketSendBufferBytesDoc = "The SO_SNDBUF buffer of the socket server sockets. If the value is -1, the OS default will be used."
  val SocketReceiveBufferBytesDoc = "The SO_RCVBUF buffer of the socket server sockets. If the value is -1, the OS default will be used."
  val SocketRequestMaxBytesDoc = "The maximum number of bytes in a socket request"
  val ConnectionsMaxIdleMsDoc = "Idle connections timeout: the server socket processor threads close the connections that idle more than this"
  val FailedAuthenticationDelayMsDoc = "Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure. " +
    s"This must be configured to be less than $ConnectionsMaxIdleMsProp to prevent connection timeout."

  /** ******** Common Security Configuration *************/
  val PrincipalBuilderClassDoc = BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC
  val ConnectionsMaxReauthMsDoc = BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_DOC
  val securityProviderClassDoc = SecurityConfig.SECURITY_PROVIDERS_DOC

  /** ********* SSL Configuration ****************/
  val SslProtocolDoc = SslConfigs.SSL_PROTOCOL_DOC
  val SslProviderDoc = SslConfigs.SSL_PROVIDER_DOC
  val SslCipherSuitesDoc = SslConfigs.SSL_CIPHER_SUITES_DOC
  val SslEnabledProtocolsDoc = SslConfigs.SSL_ENABLED_PROTOCOLS_DOC
  val SslKeystoreTypeDoc = SslConfigs.SSL_KEYSTORE_TYPE_DOC
  val SslKeystoreLocationDoc = SslConfigs.SSL_KEYSTORE_LOCATION_DOC
  val SslKeystorePasswordDoc = SslConfigs.SSL_KEYSTORE_PASSWORD_DOC
  val SslKeyPasswordDoc = SslConfigs.SSL_KEY_PASSWORD_DOC
  val SslKeystoreKeyDoc = SslConfigs.SSL_KEYSTORE_KEY_DOC
  val SslKeystoreCertificateChainDoc = SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_DOC
  val SslTruststoreTypeDoc = SslConfigs.SSL_TRUSTSTORE_TYPE_DOC
  val SslTruststorePasswordDoc = SslConfigs.SSL_TRUSTSTORE_PASSWORD_DOC
  val SslTruststoreLocationDoc = SslConfigs.SSL_TRUSTSTORE_LOCATION_DOC
  val SslTruststoreCertificatesDoc = SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_DOC
  val SslKeyManagerAlgorithmDoc = SslConfigs.SSL_KEYMANAGER_ALGORITHM_DOC
  val SslTrustManagerAlgorithmDoc = SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_DOC
  val SslEndpointIdentificationAlgorithmDoc = SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
  val SslSecureRandomImplementationDoc = SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_DOC
  val SslClientAuthDoc = BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC
  val SslPrincipalMappingRulesDoc = BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_DOC
  val SslEngineFactoryClassDoc = SslConfigs.SSL_ENGINE_FACTORY_CLASS_DOC

  /** ********* Sasl Configuration ****************/
  val SaslJaasConfigDoc = SaslConfigs.SASL_JAAS_CONFIG_DOC
  val SaslEnabledMechanismsDoc = BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC
  val SaslServerCallbackHandlerClassDoc = BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC
  val SaslClientCallbackHandlerClassDoc = SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC
  val SaslLoginClassDoc = SaslConfigs.SASL_LOGIN_CLASS_DOC
  val SaslLoginCallbackHandlerClassDoc = SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC
  val SaslKerberosServiceNameDoc = SaslConfigs.SASL_KERBEROS_SERVICE_NAME_DOC
  val SaslKerberosKinitCmdDoc = SaslConfigs.SASL_KERBEROS_KINIT_CMD_DOC
  val SaslKerberosTicketRenewWindowFactorDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
  val SaslKerberosTicketRenewJitterDoc = SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
  val SaslKerberosMinTimeBeforeReloginDoc = SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
  val SaslKerberosPrincipalToLocalRulesDoc = BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC
  val SaslLoginRefreshWindowFactorDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC
  val SaslLoginRefreshWindowJitterDoc = SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC
  val SaslLoginRefreshMinPeriodSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC
  val SaslLoginRefreshBufferSecondsDoc = SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC

  private val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Range._
    import ConfigDef.Type._
    import ConfigDef.ValidString._

    new ConfigDef()

      /** ********* General Configuration ***********/
      .define(NumNetworkThreadsProp, INT, Defaults.NumNetworkThreads, atLeast(1), HIGH, NumNetworkThreadsDoc)
      .define(QueuedMaxRequestsProp, INT, Defaults.QueuedMaxRequests, atLeast(1), HIGH, QueuedMaxRequestsDoc)
      .define(QueuedMaxBytesProp, LONG, Defaults.QueuedMaxRequestBytes, MEDIUM, QueuedMaxRequestBytesDoc)

      /** ********* Socket Server Configuration ***********/
      .define(ListenersProp, STRING, null, HIGH, ListenersDoc)
      .define(ListenerSecurityProtocolMapProp, STRING, Defaults.ListenerSecurityProtocolMap, LOW, ListenerSecurityProtocolMapDoc)
      .define(SocketSendBufferBytesProp, INT, Defaults.SocketSendBufferBytes, HIGH, SocketSendBufferBytesDoc)
      .define(SocketReceiveBufferBytesProp, INT, Defaults.SocketReceiveBufferBytes, HIGH, SocketReceiveBufferBytesDoc)
      .define(SocketRequestMaxBytesProp, INT, Defaults.SocketRequestMaxBytes, atLeast(1), HIGH, SocketRequestMaxBytesDoc)
      .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, MEDIUM, ConnectionsMaxIdleMsDoc)
      .define(FailedAuthenticationDelayMsProp, INT, Defaults.FailedAuthenticationDelayMs, atLeast(0), LOW, FailedAuthenticationDelayMsDoc)

      /** ********* General Security Configuration ****************/
      .define(ConnectionsMaxReauthMsProp, LONG, Defaults.ConnectionsMaxReauthMsDefault, MEDIUM, ConnectionsMaxReauthMsDoc)
      .define(securityProviderClassProp, STRING, null, LOW, securityProviderClassDoc)

      /** ********* SSL Configuration ****************/
      .define(PrincipalBuilderClassProp, CLASS, null, MEDIUM, PrincipalBuilderClassDoc)
      .define(SslProtocolProp, STRING, Defaults.SslProtocol, MEDIUM, SslProtocolDoc)
      .define(SslProviderProp, STRING, null, MEDIUM, SslProviderDoc)
      .define(SslEnabledProtocolsProp, LIST, Defaults.SslEnabledProtocols, MEDIUM, SslEnabledProtocolsDoc)
      .define(SslKeystoreTypeProp, STRING, Defaults.SslKeystoreType, MEDIUM, SslKeystoreTypeDoc)
      .define(SslKeystoreLocationProp, STRING, null, MEDIUM, SslKeystoreLocationDoc)
      .define(SslKeystorePasswordProp, PASSWORD, null, MEDIUM, SslKeystorePasswordDoc)
      .define(SslKeyPasswordProp, PASSWORD, null, MEDIUM, SslKeyPasswordDoc)
      .define(SslKeystoreKeyProp, PASSWORD, null, MEDIUM, SslKeystoreKeyDoc)
      .define(SslKeystoreCertificateChainProp, PASSWORD, null, MEDIUM, SslKeystoreCertificateChainDoc)
      .define(SslTruststoreTypeProp, STRING, Defaults.SslTruststoreType, MEDIUM, SslTruststoreTypeDoc)
      .define(SslTruststoreLocationProp, STRING, null, MEDIUM, SslTruststoreLocationDoc)
      .define(SslTruststorePasswordProp, PASSWORD, null, MEDIUM, SslTruststorePasswordDoc)
      .define(SslTruststoreCertificatesProp, PASSWORD, null, MEDIUM, SslTruststoreCertificatesDoc)
      .define(SslKeyManagerAlgorithmProp, STRING, Defaults.SslKeyManagerAlgorithm, MEDIUM, SslKeyManagerAlgorithmDoc)
      .define(SslTrustManagerAlgorithmProp, STRING, Defaults.SslTrustManagerAlgorithm, MEDIUM, SslTrustManagerAlgorithmDoc)
      .define(SslEndpointIdentificationAlgorithmProp, STRING, Defaults.SslEndpointIdentificationAlgorithm, LOW, SslEndpointIdentificationAlgorithmDoc)
      .define(SslSecureRandomImplementationProp, STRING, null, LOW, SslSecureRandomImplementationDoc)
      .define(SslClientAuthProp, STRING, Defaults.SslClientAuthentication, in(Defaults.SslClientAuthenticationValidValues:_*), MEDIUM, SslClientAuthDoc)
      .define(SslCipherSuitesProp, LIST, Collections.emptyList(), MEDIUM, SslCipherSuitesDoc)
      .define(SslPrincipalMappingRulesProp, STRING, Defaults.SslPrincipalMappingRules, LOW, SslPrincipalMappingRulesDoc)
      .define(SslEngineFactoryClassProp, CLASS, null, LOW, SslEngineFactoryClassDoc)

      /** ********* Sasl Configuration ****************/
      .define(SaslJaasConfigProp, PASSWORD, null, MEDIUM, SaslJaasConfigDoc)
      .define(SaslEnabledMechanismsProp, LIST, Defaults.SaslEnabledMechanisms, MEDIUM, SaslEnabledMechanismsDoc)
      .define(SaslServerCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslServerCallbackHandlerClassDoc)
      .define(SaslClientCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslClientCallbackHandlerClassDoc)
      .define(SaslLoginClassProp, CLASS, null, MEDIUM, SaslLoginClassDoc)
      .define(SaslLoginCallbackHandlerClassProp, CLASS, null, MEDIUM, SaslLoginCallbackHandlerClassDoc)
      .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, SaslKerberosServiceNameDoc)
      .define(SaslKerberosKinitCmdProp, STRING, Defaults.SaslKerberosKinitCmd, MEDIUM, SaslKerberosKinitCmdDoc)
      .define(SaslKerberosTicketRenewWindowFactorProp, DOUBLE, Defaults.SaslKerberosTicketRenewWindowFactor, MEDIUM, SaslKerberosTicketRenewWindowFactorDoc)
      .define(SaslKerberosTicketRenewJitterProp, DOUBLE, Defaults.SaslKerberosTicketRenewJitter, MEDIUM, SaslKerberosTicketRenewJitterDoc)
      .define(SaslKerberosMinTimeBeforeReloginProp, LONG, Defaults.SaslKerberosMinTimeBeforeRelogin, MEDIUM, SaslKerberosMinTimeBeforeReloginDoc)
      .define(SaslKerberosPrincipalToLocalRulesProp, LIST, Defaults.SaslKerberosPrincipalToLocalRules, MEDIUM, SaslKerberosPrincipalToLocalRulesDoc)
      .define(SaslLoginRefreshWindowFactorProp, DOUBLE, Defaults.SaslLoginRefreshWindowFactor, MEDIUM, SaslLoginRefreshWindowFactorDoc)
      .define(SaslLoginRefreshWindowJitterProp, DOUBLE, Defaults.SaslLoginRefreshWindowJitter, MEDIUM, SaslLoginRefreshWindowJitterDoc)
      .define(SaslLoginRefreshMinPeriodSecondsProp, SHORT, Defaults.SaslLoginRefreshMinPeriodSeconds, MEDIUM, SaslLoginRefreshMinPeriodSecondsDoc)
      .define(SaslLoginRefreshBufferSecondsProp, SHORT, Defaults.SaslLoginRefreshBufferSeconds, MEDIUM, SaslLoginRefreshBufferSecondsDoc)
  }

  def configNames: Seq[String] = configDef.names.asScala.toBuffer.sorted

  def fromProps(props: Properties): KafkaProxyConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): KafkaProxyConfig =
    new KafkaProxyConfig(props, doLog)

  def fromProps(defaults: Properties, overrides: Properties): KafkaProxyConfig =
    fromProps(defaults, overrides, true)

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaProxyConfig = {
    val props = new Properties()
    props.putAll(defaults)
    props.putAll(overrides)
    fromProps(props, doLog)
  }

  def apply(props: java.util.Map[_, _]): KafkaProxyConfig = new KafkaProxyConfig(props, true)

  private def typeOf(name: String): Option[ConfigDef.Type] = Option(configDef.configKeys.get(name)).map(_.`type`)

  def maybeSensitive(configType: Option[ConfigDef.Type]): Boolean = {
    // If we can't determine the config entry type, treat it as a sensitive config to be safe
    configType.isEmpty || configType.contains(ConfigDef.Type.PASSWORD)
  }
}

class KafkaProxyConfig(val props: java.util.Map[_, _], doLog: Boolean) extends AbstractConfig(KafkaProxyConfig.configDef, props, doLog) {

  def this(props: java.util.Map[_, _]) = this(props, true)

  /** ********* General Configuration ***********/
  def numNetworkThreads = getInt(KafkaProxyConfig.NumNetworkThreadsProp)
  val queuedMaxRequests = getInt(KafkaProxyConfig.QueuedMaxRequestsProp)
  val queuedMaxBytes = getLong(KafkaProxyConfig.QueuedMaxBytesProp)

  /** ********* Socket Server Configuration ***********/
  val socketSendBufferBytes = getInt(KafkaProxyConfig.SocketSendBufferBytesProp)
  val socketReceiveBufferBytes = getInt(KafkaProxyConfig.SocketReceiveBufferBytesProp)
  val socketRequestMaxBytes = getInt(KafkaProxyConfig.SocketRequestMaxBytesProp)

  val connectionsMaxIdleMs = getLong(KafkaProxyConfig.ConnectionsMaxIdleMsProp)
  val failedAuthenticationDelayMs = getInt(KafkaProxyConfig.FailedAuthenticationDelayMsProp)

  /** ********* Fetch Configuration **************/
  private def getMap(propName: String, propValue: String): Map[String, String] = {
    try {
      parseCsvMap(propValue)
    } catch {
      case e: Exception => throw new IllegalArgumentException("Error parsing configuration property '%s': %s".format(propName, e.getMessage))
    }
  }

  /**
   * This method gets comma separated values which contains key,value pairs and returns a map of
   * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
   * Also supports strings with multiple ":" such as IpV6 addresses, taking the last occurrence
   * of the ":" in the pair as the split, eg a:b:c:val1, d:e:f:val2 => a:b:c -> val1, d:e:f -> val2
   */
  def parseCsvMap(str: String): Map[String, String] = {
    val map = new mutable.HashMap[String, String]
    if ("".equals(str))
      return map
    val keyVals = str.split("\\s*,\\s*").map(s => {
      val lio = s.lastIndexOf(":")
      (s.substring(0,lio).trim, s.substring(lio + 1).trim)
    })
    keyVals.toMap
  }

  // If the user did not define listeners but did define host or port, let's use them in backward compatible way
  // If none of those are defined, we default to PLAINTEXT://:9092
  def listeners: Seq[Endpoint] = {
    Option(getString(KafkaProxyConfig.ListenersProp)).map { listenerProp =>
      try {
        val listenerList = parseCsvList(listenerProp)
        listenerList.map(Endpoint.createEndPoint(_, Some(listenerSecurityProtocolMap)))
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Error creating broker listeners from '$listeners': ${e.getMessage}", e)
      }
    }.getOrElse(Seq())
  }

  def parseCsvList(csvList: String): Seq[String] = {
    if (csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
  }

  private def getSecurityProtocol(protocolName: String, configName: String): SecurityProtocol = {
    try SecurityProtocol.forName(protocolName)
    catch {
      case _: IllegalArgumentException =>
        throw new ConfigException(s"Invalid security protocol `$protocolName` defined in $configName")
    }
  }

  def listenerSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = {
    getMap(KafkaProxyConfig.ListenerSecurityProtocolMapProp, getString(KafkaProxyConfig.ListenerSecurityProtocolMapProp))
      .map { case (listenerName, protocolName) =>
        ListenerName.normalised(listenerName) -> getSecurityProtocol(protocolName, KafkaProxyConfig.ListenerSecurityProtocolMapProp)
      }
  }

  validateValues()

  private def validateValues(): Unit = {
    require(queuedMaxBytes <= 0 || queuedMaxBytes >= socketRequestMaxBytes,
      s"${KafkaProxyConfig.QueuedMaxBytesProp} must be larger or equal to ${KafkaProxyConfig.SocketRequestMaxBytesProp}")

    if (connectionsMaxIdleMs >= 0)
      require(failedAuthenticationDelayMs < connectionsMaxIdleMs,
        s"${KafkaProxyConfig.FailedAuthenticationDelayMsProp}=$failedAuthenticationDelayMs should always be less than" +
          s" ${KafkaProxyConfig.ConnectionsMaxIdleMsProp}=$connectionsMaxIdleMs to prevent failed" +
          s" authentication responses from timing out")
  }
}
