package io.airbyte.cdk.command

import io.airbyte.cdk.operation.Operation
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Requires(property = CONNECTOR_CONFIG_PREFIX)
private class ConnectorConfigurationValidator(
    val operation: Operation,
    val configSupplier: ConnectorConfigurationSupplier<out ConnectorConfiguration>
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

@Singleton
@Requires(property = CONNECTOR_CATALOG_PREFIX)
private class ConfiguredAirbyteCatalogValidator(
    val operation: Operation,
    val catalogSupplier: ConfiguredAirbyteCatalogSupplier,
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        val hasCatalog: Boolean = ConfiguredAirbyteCatalog() != catalogSupplier.get()
        if (operation.type.requiresCatalog && !hasCatalog) {
            throw IllegalArgumentException(
                "${operation.type.name} requires a valid configured catalog, none available"
            )
        }
        if (hasCatalog) {
            logger.info { "valid configured catalog present for ${operation.type.name}" }
            if (!operation.type.requiresCatalog) {
                logger.warn { "${operation.type.name} does not require a catalog" }
            }
        }
    }
}

@Singleton
@Requires(property = CONNECTOR_STATE_PREFIX)
private class ConnectorInputStateValidator(
    val operation: Operation,
    val configSupplier: ConnectorConfigurationSupplier<out ConnectorConfiguration>,
    val stateSupplier: ConnectorInputStateSupplier,
) : ApplicationEventListener<StartupEvent> {

    override fun onApplicationEvent(event: StartupEvent) {
        val state: List<AirbyteStateMessage> = stateSupplier.get()
        if (state.isEmpty()) {
            // Input state is always optional.
            return
        }
        val stateType: AirbyteStateMessage.AirbyteStateType = state.first().type
        logger.info { "valid $stateType input state present for ${operation.type.name}" }
        if (!operation.type.acceptsState) {
            logger.warn { "${operation.type.name} does not accept state" }
            return
        }
        val expectedStateType: AirbyteStateMessage.AirbyteStateType =
            sourceConfig().expectedStateType
        if (expectedStateType != stateType) {
            throw IllegalArgumentException(
                "$stateType input state incompatible with config, which requires $expectedStateType"
            )
        }
    }

    private fun sourceConfig(): SourceConnectorConfiguration =
        configSupplier.get() as SourceConnectorConfiguration
}
