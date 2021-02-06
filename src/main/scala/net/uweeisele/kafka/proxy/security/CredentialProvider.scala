package net.uweeisele.kafka.proxy.security

import java.util.{Collection, Properties}

import org.apache.kafka.common.security.authenticator.CredentialCache
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef._
import org.apache.kafka.common.security.scram.internals.{ScramCredentialUtils, ScramMechanism}
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache

class CredentialProvider(scramMechanisms: Collection[String], val tokenCache: DelegationTokenCache) {

  val credentialCache = new CredentialCache
  ScramCredentialUtils.createCache(credentialCache, scramMechanisms)

  def updateCredentials(username: String, config: Properties): Unit = {
    for (mechanism <- ScramMechanism.values()) {
      val cache = credentialCache.cache(mechanism.mechanismName, classOf[ScramCredential])
      if (cache != null) {
        config.getProperty(mechanism.mechanismName) match {
          case null => cache.remove(username)
          case c => cache.put(username, ScramCredentialUtils.credentialFromString(c))
        }
      }
    }
  }
}

object CredentialProvider {
  def userCredentialConfigs: ConfigDef = {
    ScramMechanism.values.foldLeft(new ConfigDef) {
      (c, m) => c.define(m.mechanismName, Type.STRING, null, Importance.MEDIUM, s"User credentials for SCRAM mechanism ${m.mechanismName}")
    }
  }
}