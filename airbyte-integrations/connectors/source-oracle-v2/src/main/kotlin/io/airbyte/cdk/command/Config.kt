/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.command

import io.airbyte.cdk.operation.Operation
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.github.oshai.kotlinlogging.KotlinLogging
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
    val sshTunnel: SshTunnelMethod

    fun getDefaultNamespace(): Optional<String>
}

interface ConnectorConfigurationSupplier<T : ConnectorConfiguration> : Supplier<T>

interface SourceConnectorConfiguration : ConnectorConfiguration {

    val expectedStateType: AirbyteStateType

    val jdbcUrl: String
    val jdbcProperties: Map<String, String>

    val schemas: List<String>

    fun createConnection(): Connection =
        DriverManager.getConnection(jdbcUrl, Properties().apply { putAll(jdbcProperties) }).also {
            it.isReadOnly = true
        }
}

interface DestinationConnectorConfiguration : ConnectorConfiguration {

    fun getRawNamespace(): Optional<String>
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
            throw ConfigErrorException("invalid connector configuration", e)
        }
        logger.info { "valid connector configuration present for ${operation.type.name}" }
        if (!operation.type.requiresConfiguration) {
            logger.warn { "${operation.type.name} does not require configuration" }
        }
    }
}
