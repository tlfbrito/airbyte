/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.command

import com.fasterxml.jackson.annotation.JsonProperty
import io.airbyte.cdk.core.operation.Operation
import io.airbyte.cdk.integrations.base.ssh.SshTunnel
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import java.util.function.Supplier

private val logger = KotlinLogging.logger {}

const val CONNECTOR_CONFIG_PREFIX: String = "airbyte.connector.config"

/** Interface that defines a typed connector configuration. */
interface ConnectorConfiguration {

    val realHost: String
    val realPort: Int
    val sshTunnel: SshTunnelConfiguration

    fun getDefaultNamespace(): Optional<String>

}

interface ConnectorConfigurationSupplier<T : ConnectorConfiguration> : Supplier<T>

interface SourceConnectorConfiguration : ConnectorConfiguration {

    val expectedStateType: AirbyteStateType

    val jdbcUrl: String
    val jdbcProperties: Map<String, String>

    val schemas: List<String>

    fun createConnection(): Connection =
        DriverManager.getConnection(jdbcUrl, Properties().apply { putAll(jdbcProperties) })
            .also { it.isReadOnly = true }
}

interface DestinationConnectorConfiguration : ConnectorConfiguration {

    fun getRawNamespace(): Optional<String>

}

sealed interface SshTunnelConfiguration {
    val tunnelMethod: SshTunnel.TunnelMethod
}

data object SshNoTunnelConfiguration : SshTunnelConfiguration {
    override val tunnelMethod = SshTunnel.TunnelMethod.NO_TUNNEL
}

data class SshKeyAuthTunnelConfiguration(
    val host: String,
    val port: Int,
    val user: String,
    val key: String,
) : SshTunnelConfiguration {
    override val tunnelMethod = SshTunnel.TunnelMethod.SSH_KEY_AUTH
}

data class SshPasswordAuthTunnelConfiguration(
    val host: String,
    val port: Int,
    val user: String,
    val password: String,
) : SshTunnelConfiguration {
    override val tunnelMethod = SshTunnel.TunnelMethod.SSH_PASSWORD_AUTH
}

interface SshTunnelConfigurationSupplier : Supplier<SshTunnelConfiguration>

@ConfigurationProperties("$CONNECTOR_CONFIG_PREFIX.tunnel_method")
class SshTunnelConfigurationPOJO : SshTunnelConfigurationSupplier {

    private val validated: SshTunnelConfiguration by lazy {
        when (SshTunnel.TunnelMethod.valueOf(tunnelMethod.uppercase())) {
            SshTunnel.TunnelMethod.NO_TUNNEL -> SshNoTunnelConfiguration
            SshTunnel.TunnelMethod.SSH_KEY_AUTH -> SshKeyAuthTunnelConfiguration(tunnelHost!!, tunnelPort, tunnelUser!!, sshKey!!)
            SshTunnel.TunnelMethod.SSH_PASSWORD_AUTH -> SshPasswordAuthTunnelConfiguration(tunnelHost!!, tunnelPort, tunnelUser!!, tunnelUserPassword!!)
        }
    }

    override fun get(): SshTunnelConfiguration = validated

    @JsonProperty("tunnel_method")
    var tunnelMethod: String = "NO_TUNNEL"

    @JsonProperty("tunnel_host")
    var tunnelHost: String? = null

    @JsonProperty("tunnel_port")
    var tunnelPort: Int = 22

    @JsonProperty("tunnel_user")
    var tunnelUser: String? = null

    @JsonProperty("ssh_key")
    var sshKey: String? = null

    @JsonProperty("tunnel_user_password")
    var tunnelUserPassword: String? = null
}

@Singleton
@Requires(property = CONNECTOR_CONFIG_PREFIX)
class ConnectorConfigurationValidator(
    private val operation: Operation,
    private val configSupplier: ConnectorConfigurationSupplier<*>
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        try {
            configSupplier.get() // Force evaluation and validation of configuration.
        } catch (e: ConfigErrorException) {
            throw e
        } catch (e: Exception) {
            throw ConfigErrorException("invalid connector configuration" , e)
        }
        logger.info { "valid connector configuration present for ${operation.type.name}" }
        if (!operation.type.requiresConfiguration) {
            logger.warn { "${operation.type.name} does not require configuration" }
        }
    }
}
